{{
    config(
        database='DBT_LABS',
        schema='RAW_SCHEMA',
        materialized='table'
    )
}}

SELECT
    DELIVERY_ID, 
    CUSTOMER_ID,
    PRODUCT_ID,
    ORDER_DATE,
    DELIVERY_DATE, 
    STATUS,
    CURRENT_TIMESTAMP() AS LOADED_AT
FROM {{ source('prod_del', 'del') }}