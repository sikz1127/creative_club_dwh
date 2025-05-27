--what is the business key of this table?
--To effectively find the "same real-world human," we'll ideally need email and name
select
        "order_pos_id" as order_pos_id,
        "order_id" as  order_id,
        "product_id" as product_id,
        "product_unit" as product_unit,
        "product_name" as product_name,
        round("price",4) as price, --assumption price per unit
        round("quantity",4) as quantity,
        round(round("quantity",4) * round("price",4),4) as position_amount,
        "created_at" as created_at,
        "updated_at" as updated_at,

from {{source("webdata","raw_order_positions")}}
