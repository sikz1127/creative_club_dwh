{{
  config(
    materialized = "table"
  )
}}

select
  {{ dbt_utils.surrogate_key(['shop_id']) }} as shop_sk,
  shop_id,
  shop,
  platform,
  locale,
  shop_locale,
  platform_type
from {{ ref('stg_shops') }}
