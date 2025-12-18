{% macro return_status(flag_column) %}
    CASE
        WHEN {{flag_column}} = 'TRUE' THEN 'Returned'
        ELSE 'Completed'
    END
{% endmacro %}(flag_column)