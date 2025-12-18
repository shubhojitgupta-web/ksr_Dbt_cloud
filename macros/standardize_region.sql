{% macro standardize_region(region_column) %}
    INITCAP({{region_column}})
{% endmacro %}