{% macro mask_email(email_column) %}
    CONCAT(
        'XXXXXXXX',
        SUBSTR({{ email_column }}, POSITION('@' IN {{ email_column }}))
    )
{% endmacro %}