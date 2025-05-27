--what is the business key of this table?
--To effectively find the "same real-world human," we'll ideally need email and name
select
        "product_id" as product_id,
        "sku_id" as  sku_id,
        "product_name" as product_name,
        "product_number" as product_number,
        "variant_name" as variant_name,
        "is_variant" as is_variant,
        "product_state_desc" as product_state_desc,
        "first_published_at" as first_published_at,
        "created_at" as created_at,
        "updated_at" as updated_at,

from {{source("webdata","raw_products")}}
