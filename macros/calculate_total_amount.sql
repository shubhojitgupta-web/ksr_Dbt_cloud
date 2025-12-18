{% macro calculate_total_amount(quantity_column, price_column) %}
    {{quantity_column}} * {{price_column}}
{% endmacro %}