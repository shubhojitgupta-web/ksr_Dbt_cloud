{% macro age_band(age_column) %}
    CASE 
        WHEN {{age_column}} < 25 THEN 'Youth'
        WHEN {{age_column}} BETWEEN 25 AND 45 THEN 'Adult'
        ELSE 'Senior'
    END
{% endmacro %}