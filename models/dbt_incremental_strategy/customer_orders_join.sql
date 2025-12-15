SELECT 
    O.ORDER_ID,
    O.CUSTOMER_ID,
    O.ORDER_DATE,
    O.ORDER_STATUS,
    O.AMOUNT,
    O.PAYMENT_MODE,
    C.FULL_NAME,
    C.CITY,
    C.STATE
FROM {{ source('cust_master', 'cust_ord') }} O
INNER JOIN {{ source('cust_master', 'cust') }} C
    ON C.CUSTOMER_ID = O.CUSTOMER_ID
WHERE C.STATUS = 'active'

{% if is_incremental() %}
  AND O.ORDER_DATE >= dateadd(day, -30, current_date)
{% endif %}