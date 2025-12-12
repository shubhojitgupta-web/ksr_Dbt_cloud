{{
    config(
        database='DBT_LABS',
        schema= 'SILVER_SCH',
        materialized='table'
    )
}}

SELECT
    C.ID AS CUSTOMER_ID,
    C.FIRST_NAME,
    C.LAST_NAME,
    P.PRODUCT_ID,
    P.PRODUCT_NAME,
    UPPER(P.CATEGORY) AS CATEGORY,
    P.PRICE,
    D.DELIVERY_ID,
    D.ORDER_DATE,
    D.DELIVERY_DATE,
    D.STATUS AS DELIVERY_STATUS
FROM {{ source('customer', 'cust') }} C
JOIN {{ source('prod_del', 'del') }} D
ON C.ID = D.CUSTOMER_ID
JOIN {{ source('prod_del', 'prod') }} P
ON D.PRODUCT_ID = P.PRODUCT_ID