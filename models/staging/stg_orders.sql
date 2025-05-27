--what is the business key of this table?
--To effectively find the "same real-world human," we'll ideally need email and name
select
      "order_id" as  order_id,
        "order_number" as order_number,
        "webshop_order_number" as webshop_order_number,
        "sales_event" as sales_event,
        "order_type" as order_type,
        "customer_id" as customer_id,
        "shop_id" as shop_id,
        "payment_method" as payment_method,
        "order_date" as order_date,
        "delivery_date" as delivery_date,
        "booking_date" as booking_date,
        "created_at" as created_at,
        "updated_at" as updated_at,

from {{source("webdata","raw_orders")}}
