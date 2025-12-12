{{
    config(
        database='DBT_LABS',
        schema='SILVER_SCH',
        materialized='table'
    )
}}

SELECT
    P.PRODUCT_ID,
    P.PRODUCT_NAME,
    UPPER(P.CATEGORY) AS CATEGORY,
    P.PRICE AS UNIT_PRICE,
    D.DELIVERY_ID,
    D.ORDER_DATE,
    TO_DATE(TO_CHAR(D.DELIVERY_DATE, 'DD-MM-YYYY'),'DD-MM-YYYY') AS DELIVERY_DATE,
    D.STATUS AS DELIVERY_STATUS
FROM {{ source('prod_del', 'prod') }} P
JOIN {{ source('prod_del', 'del') }} D
ON D.PRODUCT_ID = P.PRODUCT_ID