{{
    config(
        database='DBT_LABS',
        schema='RAW_SCHEMA',
        materialized='table'
    )
}}

SELECT
    ID AS CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    CURRENT_TIMESTAMP() AS LOADED_AT
FROM {{ source('customer', 'cust') }}