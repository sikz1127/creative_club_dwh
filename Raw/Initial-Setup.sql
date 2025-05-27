-- 1. Create a warehouse
CREATE OR REPLACE WAREHOUSE transform_wh
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- 2. Create a resource monitor

CREATE OR REPLACE RESOURCE MONITOR rm_transform_wh WITH CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 80 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE transform_wh SET RESOURCE_MONITOR = rm_transform_wh;


-- 3. Create a role for database administration
CREATE OR REPLACE ROLE tcc_dba;

-- 4. Grant privileges to the role
GRANT USAGE ON WAREHOUSE transform_wh TO ROLE tcc_dba;
GRANT OPERATE ON WAREHOUSE transform_wh TO ROLE tcc_dba;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE tcc_dba;
GRANT EXECUTE DATA METRIC FUNCTION on account to role tcc_dba;
GRANT application role SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER
    to role tcc_dba;

GRANT ROLE tcc_dba TO USER KATALINATCC;
-- 5. Set current role to tcc_dba
USE ROLE tcc_dba;

-- 6. Create a database and schemas
CREATE OR REPLACE DATABASE tcc_analytics;
USE DATABASE tcc_analytics;

CREATE OR REPLACE SCHEMA raw;
CREATE OR REPLACE SCHEMA core;
CREATE OR REPLACE SCHEMA mart;

-- 7. Use raw schema and create internal stage
USE SCHEMA raw;
CREATE OR REPLACE STAGE raw_data;
USE WAREHOUSE TRANSFORM_WH;

-- Step 1: Create a clean CSV file format
CREATE OR REPLACE FILE FORMAT raw.raw_csv_file_format
    TYPE = 'CSV'
        FIELD_DELIMITER = ','
        --SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        TRIM_SPACE = TRUE
        REPLACE_INVALID_CHARACTERS = TRUE
        DATE_FORMAT = 'AUTO'
        TIME_FORMAT = 'AUTO'
        TIMESTAMP_FORMAT = 'AUTO'
        EMPTY_FIELD_AS_NULL = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE PARSE_HEADER = TRUE;

-- Step 2: Create the table with schema evolution enabled, inferring from the CSV file
CREATE OR REPLACE TABLE raw.raw_customers
    ENABLE_SCHEMA_EVOLUTION = TRUE
    USING TEMPLATE
(
    SELECT
    ARRAY_AGG(
    object_construct
(
    *
))
    FROM TABLE
(
    INFER_SCHEMA
(
    LOCATION
    =>
    '@raw_data/customers.csv',
    FILE_FORMAT
    =>
    'raw.raw_csv_file_format'
)
    )
    );

desc table raw.raw_customers;

-----------For automation use Snowpipe and external e.g. S3 stage, also event notification can be added
COPY INTO raw.raw_customers
    FROM @raw_data/customers.csv
    FILE_FORMAT = raw.raw_csv_file_format,
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT *
FROM raw.raw_customers
limit 10;

CREATE OR REPLACE TABLE raw.raw_products
    ENABLE_SCHEMA_EVOLUTION = TRUE
    USING TEMPLATE
(
    SELECT
    ARRAY_AGG(
    object_construct
(
    *
))
    FROM TABLE
(
    INFER_SCHEMA
(
    LOCATION
    =>
    '@raw_data/products.csv',
    FILE_FORMAT
    =>
    'raw.raw_csv_file_format'
)
    )
    );
desc table raw.raw_products;

COPY INTO raw.raw_products
    FROM @raw_data/products.csv
    FILE_FORMAT = raw.raw_csv_file_format,
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
select *
from raw.raw_products
limit 10;


CREATE OR REPLACE TABLE raw.raw_orders
    ENABLE_SCHEMA_EVOLUTION = TRUE
    USING TEMPLATE
(
    SELECT
    ARRAY_AGG(
    object_construct
(
    *
))
    FROM TABLE
(
    INFER_SCHEMA
(
    LOCATION
    =>
    '@raw_data/orders.csv',
    FILE_FORMAT
    =>
    'raw.raw_csv_file_format'
)
    )
    );

desc table raw.raw_orders;

COPY INTO raw.raw_orders
    FROM @raw_data/orders.csv
    FILE_FORMAT = raw.raw_csv_file_format,
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

select *
from raw.raw_orders
limit 10;
-------------------------------Order Positions
CREATE OR REPLACE TABLE raw.raw_order_positions
    ENABLE_SCHEMA_EVOLUTION = TRUE
    USING TEMPLATE
(
    SELECT
    ARRAY_AGG(
    object_construct
(
    *
))
    FROM TABLE
(
    INFER_SCHEMA
(
    LOCATION
    =>
    '@raw_data/order_positions.csv',
    FILE_FORMAT
    =>
    'RAW.raw_csv_file_format'
)
    )
    );

desc table raw.raw_order_positions;

COPY INTO raw.raw_order_positions
    FROM @raw_data/order_positions.csv
    FILE_FORMAT = raw.raw_csv_file_format,
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

select *
from raw.raw_order_positions
limit 10;

-------------------------------shops
CREATE OR REPLACE TABLE raw.raw_shops
    ENABLE_SCHEMA_EVOLUTION = TRUE
    USING TEMPLATE
(
    SELECT
    ARRAY_AGG(
    object_construct
(
    *
))
    FROM TABLE
(
    INFER_SCHEMA
(
    LOCATION
    =>
    '@raw_data/shops.csv',
    FILE_FORMAT
    =>
    'raw.raw_csv_file_format'
)
    )
    );

desc table raw.raw_shops;

COPY INTO raw.raw_shops
    FROM @raw_data/shops.csv
    FILE_FORMAT = raw.raw_csv_file_format,
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

select *
from raw.raw_shops
limit 10;
----------------------------------------------Data Analysis after insert----------------------------------------------------------------------
-- Revenue per day (only shipped orders)
SELECT DATE("delivery_date") AS date,
       COUNT("order_id")     AS shipped_orders,
       sum("total_amount")   AS revenue
FROM raw.raw_orders oraw
WHERE "delivery_date" IS NOT NULL
GROUP BY 1;
desc table raw.raw_orders;

---Check Article data
select count("sku_id")                                                                           as CNT
     , count("product_id")                                                                       as P_CNT
     , LISTAGG(DISTINCT "variant_name", ', ') WITHIN GROUP (ORDER BY "variant_name")             as variant_name_list
     , "product_name"
     , "is_variant"
     , LISTAGG(DISTINCT "product_state_desc", ', ')
               WITHIN GROUP (ORDER BY "product_state_desc")                                      as product_state_desc_list
     , LISTAGG(DISTINCT "first_published_at", ', ')
               WITHIN GROUP (ORDER BY "first_published_at")                                      as first_published_at_list
     , LISTAGG(DISTINCT "updated_at", ', ') WITHIN GROUP (ORDER BY "updated_at")                 as updated_at_list
from raw.RAW_PRODUCTS
where "is_variant" = false
group by "product_name", "is_variant"
having CNT > 1;

-- Analytical query, not a typical dbt test for "failure"
-- but for exploration of potential shared addresses, It doesn't necessarily mean an error, as multiple distinct individuals can live at the same address.
SELECT
    "address_hash_id" as address_hash_id,
    COUNT(DISTINCT "customer_id") as count_customer_ids,
    LISTAGG(DISTINCT "customer_id", ', ') WITHIN GROUP (ORDER BY "customer_id") as customer_id_list
FROM  RAW.CUSTOMERS_RAW
GROUP BY address_hash_id
HAVING COUNT(DISTINCT "customer_id") > 1;