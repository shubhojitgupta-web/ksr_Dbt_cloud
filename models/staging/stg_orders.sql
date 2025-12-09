{{
    config(
        database = 'DBT_DB',
        schema = 'STAGING',
        materialized='table'
    )
}}

SELECT
    ID, 
    USER_ID,
    ORDER_DATE
FROM {{ source('data_feed', 'orders') }}