{{
  config(
    materialized = 'incremental',
    unique_key   = 'customer_id'
  )
}}

with source_data as (
  select
    customer_id,
    address_hash_id,
    country,
    created_at,
    updated_at,
    currency_unit,
    tax_eucountry
  from {{ ref('stg_customers') }}

  {%- if is_incremental() %}
    where updated_at > (
      select max(updated_at) from {{ this }}
    )
  {%- endif %}
),

ranked as (
  select
    *,
    row_number() over (
      partition by customer_id
      order by updated_at desc
    ) as rn
  from source_data
)

select
  {{ dbt_utils.surrogate_key(['customer_id']) }}   as customer_sk,
  customer_id,
  address_hash_id,
  country,
  created_at,
  updated_at,
  currency_unit,
  tax_eucountry
from ranked
where rn = 1
