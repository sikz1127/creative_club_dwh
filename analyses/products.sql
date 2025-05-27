use role tcc_dba;
use database TCC_ANALYTICS;

--skuid_id is the same as product_id
--product_number: Seems to represent the "base product" or "parent product"
--sku_id and product_id: Represent the specific variants when is_variant = TRUE. (e.g., "Awesome T-Shirt, Blue, L"
--The Anomaly (when is_variant = FALSE)-- Some product_numbers are duplicated. but different updated_at
--What does is_variant = FALSE really mean if product_number can still have multiple distinct sku_id/product_id rows?
--  Could these be historical versions of the base product information? The changing updated_at supports this.
--  Sourće not immediately obvious from the data alone
select count(*),
       count(distinct product_id),
       LISTAGG(distinct product_id, ',') WITHIN GROUP ( ORDER BY product_id )               as product_id_LIST,
       count(distinct sku_id),
       LISTAGG(distinct sku_id, ',') WITHIN GROUP ( ORDER BY sku_id )                       as sku_id_LIST,
       product_number,
       product_name,
       LISTAGG(distinct VARIANT_NAME, ',') WITHIN GROUP (ORDER BY VARIANT_NAME)             as product_variance_list,
       LISTAGG(distinct IS_VARIANT, ',') WITHIN GROUP ( ORDER BY IS_VARIANT )               as IS_VARIANT_LIST,
       LISTAGG(distinct UPDATED_AT, ',') WITHIN GROUP ( ORDER BY UPDATED_AT )               as UPDATE_AT_LIST,
       LISTAGG(distinct PRODUCT_STATE_DESC, '') WITHIN GROUP ( ORDER BY PRODUCT_STATE_DESC) as PRODUCT_STATE_LIST
from WEB_STG.stg_products
where IS_VARIANT = false
group by product_name, product_number
having count('*') > 1
;


select count(*),
       count(distinct product_id),
       count(distinct sku_id),
       product_number,
       product_name,
       LISTAGG(distinct VARIANT_NAME, ',') WITHIN GROUP (ORDER BY VARIANT_NAME)             as product_variance_list,
       LISTAGG(distinct IS_VARIANT, ',') WITHIN GROUP ( ORDER BY IS_VARIANT )               as IS_VARIANT_LIST,
       LISTAGG(distinct UPDATED_AT, ',') WITHIN GROUP ( ORDER BY UPDATED_AT )               as UPDATE_AT_LIST,
       LISTAGG(distinct PRODUCT_STATE_DESC, '') WITHIN GROUP ( ORDER BY PRODUCT_STATE_DESC) as PRODUCT_STATE_LIST
from WEB_STG.stg_products
where IS_VARIANT = true
group by product_name, product_number
having count('*') > 1
;
