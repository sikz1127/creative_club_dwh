{{
  config(
    materialized='table'
  )
}}

with tagged_orders as (
  select
    customer_id,
    to_date(order_date) as order_date,
    -- compute each customer’s first-ever order in the same scan
    min(to_date(order_date))     over (partition by customer_id) as first_order_date
  from {{ ref('f_orders') }}
)

select
  order_date                      as date,
  -- absolute counts
  count(distinct case
                   when order_date = first_order_date
                   then customer_id
                 end)           as new_customers,
  count(distinct customer_id)     as total_customers,
  count(distinct case
                   when order_date > first_order_date
                   then customer_id
                 end)           as returning_customers,
  -- relative shares
  round(
    count(distinct case
                   when order_date = first_order_date
                   then customer_id
                 end)
    / nullif(count(distinct customer_id),0)
  , 2)                             as new_customer_share,
  round(
    count(distinct case
                   when order_date > first_order_date
                   then customer_id
                 end)
    / nullif(count(distinct customer_id),0)
  , 2)                             as returning_customer_share
from tagged_orders
group by date
order by date
