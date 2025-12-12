{{
    config(
        database='DBT_LABS',
        schema='RAW_SCHEMA',
        materialized='table'
    )
}}

SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    PRICE,
    CURRENT_TIMESTAMP() AS LOADED_AT
FROM {{ source('prod_del', 'prod') }}