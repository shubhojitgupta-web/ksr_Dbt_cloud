{% test date_validation(model, column_name) %}
    SELECT {{column_name}}
    FROM {{ model }}
    WHERE
        {{ column_name }} < DATEADD(year, -5, current_date)
    OR {{ column_name }} > CURRENT_DATE
{% endtest %}