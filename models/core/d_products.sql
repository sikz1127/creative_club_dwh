{{
  config(
    materialized = 'incremental',
    unique_key   = 'product_id'
  )
}}

with source_data as (select product_id,
                            sku_id,
                            product_name,
                            product_number,
                            variant_name,
                            is_variant,
                            product_state_desc,
                            first_published_at,
                            created_at,
                            updated_at
                     from {{ ref('stg_products') }}
                         {%- if is_incremental() %}
                     where updated_at > (select max(updated_at)         from {{ this }})
                        {%- endif %}),
     products_latest_non_variant AS (SELECT product_id,
                                            sku_id,
                                            product_name,
                                            product_number,
                                            variant_name,
                                            is_variant,
                                            product_state_desc,
                                            first_published_at,
                                            created_at,
                                            updated_at
                                     FROM (SELECT product_id,
                                                  sku_id,
                                                  product_name,
                                                  product_number,
                                                  variant_name,
                                                  is_variant,
                                                  product_state_desc,
                                                  first_published_at,
                                                  created_at,
                                                  updated_at,
                                                  ROW_NUMBER() OVER (PARTITION BY product_number  ORDER BY updated_at DESC) as rn --baseproduc
                                           FROM source_data
                                           WHERE is_variant = FALSE -- Apply this logic only to non-variants if that's the issue
                                          )
                                     WHERE rn = 1),

     all_variants AS (SELECT product_id,
                             sku_id,
                             product_name,
                             product_number,
                             variant_name,
                             is_variant,
                             product_state_desc,
                             first_published_at,
                             created_at,
                             updated_at
                      FROM source_data
                      WHERE is_variant = TRUE),
     ranked as (select product_id,
                       sku_id,
                       product_name,
                       product_number,
                       variant_name,
                       is_variant,
                       product_state_desc,
                       first_published_at,
                       created_at,
                       updated_at,
                       row_number() over ( partition by product_id order by updated_at desc) as rn
                from (SELECT product_id,
                             sku_id,
                             product_name,
                             product_number,
                             variant_name,
                             is_variant,
                             product_state_desc,
                             first_published_at,
                             created_at,
                             updated_at
                      FROM products_latest_non_variant
                      UNION ALL
                      SELECT product_id,
                             sku_id,
                             product_name,
                             product_number,
                             variant_name,
                             is_variant,
                             product_state_desc,
                             first_published_at,
                             created_at,
                             updated_at
                      FROM all_variants))

select {{ dbt_utils.surrogate_key(['product_id']) }} as product_sk,
       product_id,
       sku_id,
       product_name,
       product_number,
       variant_name,
       is_variant,
       product_state_desc,
       first_published_at,
       created_at,
       updated_at
from ranked
where rn = 1
