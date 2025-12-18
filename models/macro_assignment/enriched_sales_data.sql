WITH ORDER_RANK AS (
    SELECT 
        C.CUSTOMER_ID, 
        O.ORDER_ID,
        C.NAME, 
        {{ standardize_region('C.REGION') }} AS STANDARDIZED_REGION,
        {{mask_email('C.EMAIL')}} AS MASKED_EMAIL,
        {{age_band('C.AGE')}} AS AGE_BAND,
        O.CATEGORY,
        {{calculate_total_amount('O.QUANTITY','O.PRICE_PER_UNIT')}} AS TOTAL_AMOUNT,
        {{return_status('O.IS_RETURNED')}} AS STATUS,
        C.JOINED_DATE, 
        O.ORDER_DATE, 
        {{latest_order_per_customer('O.CUSTOMER_ID', 'O.ORDER_DATE')}} as LATEST_ORDER 
        FROM {{ source('RETAIL', 'RET_CUST') }} C
        JOIN {{ source('RETAIL', 'RET_ORD') }} O
        ON C.CUSTOMER_ID = O.CUSTOMER_ID
)
SELECT CUSTOMER_ID, ORDER_ID, NAME, 
STANDARDIZED_REGION AS REGION, 
MASKED_EMAIL AS EMAIL, AGE_BAND, CATEGORY, TOTAL_AMOUNT, STATUS, JOINED_DATE, ORDER_DATE from ORDER_RANK WHERE LATEST_ORDER = 1