--what is the business key of this table?
--To effectively find the "same real-world human," we'll ideally need email and name
select "shop_id"       as shop_id,
       "shop"          as shop,
       "platform"      as platform,
       "locale"        as locale,
       "shop_locale"   as shop_locale,
       "platform_type" as platform_type
       --currrent_date as created_at,
       --currrent_date as updated_at, --take care about timezone

from {{source("webdata","raw_shops")}}
