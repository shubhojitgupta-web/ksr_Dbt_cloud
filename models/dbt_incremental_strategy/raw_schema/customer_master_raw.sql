{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

select 
        CUSTOMER_ID,
        FULL_NAME,
        EMAIL,
        CITY,
        STATE,
        REGISTERED_ON,
        STATUS
    from {{ source('cust_master', 'cust') }}

{% if is_incremental() %}
    where REGISTERED_ON >= dateadd(day, -30, current_date) 
{% endif %}