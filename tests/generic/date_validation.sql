{% test date_validation(model, column_name, max_years_old) %}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE
    {{ column_name }} > CURRENT_DATE
    OR {{ column_name }} < DATEADD(year, -{{ max_years_old }}, CURRENT_DATE)

{% endtest %}