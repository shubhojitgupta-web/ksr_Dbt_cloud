{{
    config(
        database = 'DBT_DB',
        schema = 'STAGING',
        materialized='table'
    )
}}

SELECT
    C.CUSTOMER_ID,
    C.FIRST_NAME,
    C.LAST_NAME,
    O.ID AS ORDER_ID,
    O.ORDER_DATE,
    O.STATUS
FROM {{ ref('stg_customer') }} C
join {{ source('data_feed', 'orders') }} O
on C.CUSTOMER_ID = O.USER_ID