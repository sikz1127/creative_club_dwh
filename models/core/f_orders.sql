{{
  config(
    materialized = 'incremental',
    unique_key   = 'order_id'
  )
}}

select *,
       TO_NUMBER(TO_CHAR(order_date, 'yyyymmdd'))   as order_date_id,
       TO_NUMBER(TO_CHAR(booking_date, 'yyyymmdd')) as booking_date_id,
       TO_NUMBER(TO_CHAR(delivery_date, 'yyyymmdd')) as delivery_date_id
from {{ ref('stg_orders') }}
    {% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}

