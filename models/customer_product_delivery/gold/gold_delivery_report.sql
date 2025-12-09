{{
    config(
        database='DBT_CLEAN',
        schema='CLEAN_SCHEMA',
        materialized='view'
    )
}}

SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    PRODUCT_NAME,
    PRICE,
    ORDER_DATE,
    DELIVERY_DATE,
    DELIVERY_ID,
    DELIVERY_STATUS
FROM {{ ref('customer_product_delivery_status') }}
WHERE DELIVERY_STATUS <> 'Pending'