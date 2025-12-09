{{
    config(
        database = 'DBT_DB',
        schema = 'STAGING',
        materialized='table'
    )
}}

SELECT
    ID AS CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME
FROM {{ source('data_feed', 'cust') }}