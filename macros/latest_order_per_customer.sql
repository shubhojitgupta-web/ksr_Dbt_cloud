{% macro latest_order_per_customer(CUSTOMER_ID, ORDER_DATE) %}

    RANK() OVER (PARTITION BY {{CUSTOMER_ID}} ORDER BY {{ORDER_DATE}} DESC)

{% endmacro %}