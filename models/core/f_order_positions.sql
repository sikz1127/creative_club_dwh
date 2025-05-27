{{
  config(
    materialized = "incremental",
    unique_key   = "order_pos_id"
  )
}}

with source_data as (

  select
    order_pos_id,
    order_id,
    product_id,
    product_unit,
    product_name,
    price,
    quantity,
    position_amount,
    created_at,
    updated_at

  from {{ ref('stg_order_positions') }}

  {%- if is_incremental() %}
    where updated_at > (
      select max(updated_at) from {{ this }}
    )
  {%- endif %}

)

select
  order_pos_id,
  order_id,
  product_id,
  product_unit,
  product_name,
  price,
  quantity,
  position_amount,
  created_at,
  updated_at
from source_data
