{{
  config(
    materialized='table'
  )
}}
WITH shop_orders_positions as (SELECT ord.shop_id,
                                      s.shop,
                                      s.platform,
                                      ord.order_date_id, -- Use this for the "date" dimension for intake
                                      ord.delivery_date_id,
                                      ord.order_date,    -- Use this for the "date" dimension for intake
                                      ord.delivery_date,
                                      ord.sales_event,
                                      pos.quantity,
                                      pos.price,         -- Assuming this is price_per_unit

                                      -- Order Intake Amount
                                      CASE
                                          WHEN ord.sales_event IN ('webshop_order', 'manual_order',
                                                                   're_order' /*, add other intake events */)
                                              THEN position_amount
                                          ELSE 0
                                          END AS order_intake_amount,

                                      -- Gross Revenue Amount
                                      CASE
                                          WHEN ord.sales_event = 'shipped'
                                              THEN position_amount
                                          ELSE 0
                                          END AS gross_revenue_amount,

                                      -- Return Amount
                                      CASE
                                          WHEN ord.sales_event = 'return'
                                              THEN position_amount -- Usually returns are negative, adjust sign as needed
                                          ELSE 0
                                          END AS returned_amount,

                                      -- Credit Note Amount
                                      CASE
                                          WHEN ord.sales_event = 'credit_note'
                                              THEN position_amount -- Usually credits are negative
                                          ELSE 0
                                          END AS credit_note_amount
                               FROM {{ ref('f_orders') }} ord
                                        INNER JOIN {{ ref('f_order_positions') }} pos
                                                   ON ord.order_id = pos.order_id
                                        INNER JOIN {{ ref('d_shops') }} s
                                                   ON ord.shop_id = s.shop_id
                                        LEFT JOIN {{ ref('d_date') }} dd_order
                                                  ON ord.order_date_id = dd_order.dwh_date_id
                                        LEFT JOIN {{ ref('d_date') }} dd_ship
                                                  ON ord.delivery_date_id = dd_ship.dwh_date_id)


SELECT
    -- Time Dimension
    fo.order_date_id,
    fo.delivery_date_id,
    -- Shop Dimension
    fo.shop_id,
    fo.shop,
    fo.platform,
    -- Metris
    SUM(fo.order_intake_amount)                                                           AS total_order_intake,
    --SUM(fo.gross_revenue_amount)  - SUM(fo.returned_amount)  -     SUM(fo.credit_note_amount)  AS total_gross_revenue,
    (SUM(fo.gross_revenue_amount) - SUM(fo.returned_amount) - SUM(fo.credit_note_amount)) AS total_net_revenue,
    -- Lag time in days
    CASE
        WHEN fo.delivery_date IS NOT NULL AND fo.order_date IS NOT NULL
            THEN DATEDIFF(day, fo.order_date, fo.delivery_date)
        ELSE NULL
        END                                                                               AS shipping_lag_days,
    round(
            case
                when total_order_intake > 0
                    then 1 - total_net_revenue / total_order_intake
                else null
                end
        , 4)                                                                              as drop_off_rate,

FROM shop_orders_positions fo

GROUP BY fo.order_date_id,
         fo.order_date,
         fo.delivery_date_id,
         fo.delivery_date,
         fo.shop_id,
         fo.shop,
         fo.platform
ORDER BY fo.order_date_id,
         fo.shop