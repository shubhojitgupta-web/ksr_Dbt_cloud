{{
    config(
        database='DBT_CLEAN',
        schema='CLEAN_SCHEMA',
        materialized='table'
    )
}}

SELECT
    PRODUCT_ID, 
    PRODUCT_NAME, 
    CATEGORY, 
    UNIT_PRICE, 
    DELIVERY_DATE, 
    CASE 
        WHEN DELIVERY_STATUS = 'Delivered' THEN 'Yes'
        ELSE 'No'
    END AS IS_DELIVERED
FROM {{ ref('product_delivery_info') }}
ORDER BY DELIVERY_DATE DESC