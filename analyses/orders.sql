-----------------Orders
select *
from web_stg.stg_orders
where CUSTOMER_ID = 'st-3532732';
/*
63206127
63218012
*/
select round(QUANTITY * price, 2) as REVENUE,
       *
from WEB_STG.STG_ORDER_POSITIONS
where ORDER_ID = '63206127';
---- not clear so use price per unit
select avg(round(QUANTITY * price, 2)) as REVENUE,
       min(round(QUANTITY * price, 2)) as MIN_REVENUE,
       max(round(QUANTITY * price, 2)) as MAX_REVENUE,
       count(distinct ORDER_ID)        as ORDER_COUNT,
       sum(round(QUANTITY * price, 2)) as TOTAL_REVENUE,
       TOTAL_REVENUE / order_count     as AVERAGE_REVENUE_PER_ORDER
from WEB_STG.STG_ORDER_POSITIONS;

select avg(IFF(QUANTITY = 0, 0, round(price / QUANTITY, 2)))     as REVENUE,
       min(IFF((QUANTITY) = 0, 0, round(price / QUANTITY, 2)))   as MIN_REVENUE,
       max(IFF((QUANTITY) = 0, 0, round(price / QUANTITY, 2)))   as MAX_REVENUE,
       count(distinct ORDER_ID)                                  as ORDER_COUNT,
       sum(IFF((QUANTITY) = 0, 0, (round(price / QUANTITY, 2)))) as TOTAL_REVENUE,
       TOTAL_REVENUE / order_count                               as AVERAGE_REVENUE_PER_ORDER
from WEB_STG.STG_ORDER_POSITIONS;

select *
from WEB_STG.STG_ORDER_POSITIONS
where QUANTITY * PRICE > 14600;
--check revenue based on order status or sales event assumed to be the order state
/*webshop_order
shipped
return
invoice_in_advance
credit_note
failed_payment
internal_purposes
manual_order
collective_bill
b2b_offer
manual_invoice
re_order
*/
select count(*),
       sum(QUANTITY * price)                                                             as REVENUE_TOTAL,
       sum(case when ord.SALES_EVENT = 'shipped' then QUANTITY * price else 0 end)       as REVENUE_SHIPPED,
       sum(case when ord.SALES_EVENT = 'return' then QUANTITY * price else 0 end)        as REVENUE_RETURNED,
       sum(case when ord.SALES_EVENT = 'webshop_order' then QUANTITY * price else 0 end) as REVENUE_INTAKE,
       sum(case
               when ord.SALES_EVENT not in ('returned', 'shipped', 'webshop_order') then QUANTITY * price
               else 0 end)                                                               as REVENUE_ELSE
from web_stg.stg_orders ord
         inner join web_stg.stg_order_positions pos
                    on ord.order_id = pos.order_id
where ord.customer_id = 'st-3532732';

select count(*),
       SALES_EVENT
from web_stg.stg_orders
group by SALES_EVENT;

select *
from web_stg.stg_orders ord
         inner join web_stg.stg_order_positions pos
                    on ord.order_id = pos.order_id
where ord.customer_id = 'st-3532732';


-- In f_orders.sql or an intermediate model

SELECT ord.shop_id,
       s.SHOP,
       ord.order_date,    -- Use this for the "time" dimension for intake
       ord.delivery_date, -- Or another date representing when status became 'shipped' for revenue
       ord.sales_event,
       pos.quantity,
       pos.price,         -- Assuming this is price_per_unit

       -- Order Intake Amount
       CASE
           WHEN ord.sales_event IN ('webshop_order', 'manual_order', 're_order' /*, add other intake events */)
               THEN (pos.quantity * pos.price)
           ELSE 0
           END AS order_intake_amount,

       -- Gross Revenue Amount
       CASE
           WHEN ord.sales_event = 'shipped'
               THEN (pos.quantity * pos.price)
           ELSE 0
           END AS gross_revenue_amount,

       -- Return Amount
       CASE
           WHEN ord.sales_event = 'return'
               THEN - (pos.quantity * pos.price) -- Usually returns are negative, adjust sign as needed
           ELSE 0
           END AS returned_amount,

       -- Credit Note Amount
       CASE
           WHEN ord.sales_event = 'credit_note'
               THEN - (pos.quantity * pos.price) -- Usually credits are negative
           ELSE 0
           END AS credit_note_amount
-- ... other measures and attributes
FROM web_core.f_orders ord -- Assuming column name is sales_event in stg_orders
         INNER JOIN web_core.f_ORDER_POSITIONS pos ON ord.order_id = pos.order_id
         INNER JOIN web_core.d_shops s ON ord.shop_id = s.shop_id
         LEFT JOIN web_core.d_date dd ON TO_NUMBER(TO_CHAR(ord.order_date, 'yyyymmdd')) = dd.dwh_date_id
         LEFT JOIN web_core.D_DATE dd_ship ON TO_NUMBER(TO_CHAR(ord.delivery_date, 'yyyymmdd')) = dd_ship.dwh_date_id
where ord.customer_id = 'st-3532732'
;--ord.sales_event in ( 'credit_note', 'return') ;--ord.customer_id = 'st-3532732';

with tagged_orders as (select customer_id,
                              to_date(order_date)                                      as order_date,
                              -- compute each customer’s first-ever order in the same scan
                              min(to_date(order_date)) over (partition by customer_id) as first_order_date
                       from web_core.f_orders
                       where order_date is not null
                         and customer_id in ('st-3532732', 'st-1474786'))

select order_date                  as date,
       -- absolute counts
       count(distinct case
                          when order_date = first_order_date
                              then customer_id
           end)                    as new_customers,
       count(distinct customer_id) as total_customers,
       count(distinct case
                          when order_date > first_order_date
                              then customer_id
           end)                    as returning_customers,
       -- relative shares
       round(
               count(distinct case
                                  when order_date = first_order_date
                                      then customer_id
                   end)
                   / nullif(count(distinct customer_id), 0)
           , 2)                    as new_customer_share,
       round(
               count(distinct case
                                  when order_date > first_order_date
                                      then customer_id
                   end)
                   / nullif(count(distinct customer_id), 0)
           , 2)                    as returning_customer_share
from tagged_orders
group by date
order by date;