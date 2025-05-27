--what is the business key of this table?
--To effectively find the "same real-world human," we'll ideally need email and name
select
      "customer_id" as  customer_id,
        "address_hash_id" as address_hash_id,
        "country" as country,
        "created_at" as created_at,
        "updated_at" as updated_at,
        "currency_unit" as currency_unit,
        "tax_eucountry" as tax_eucountry
from {{source("webdata","raw_customers")}}
