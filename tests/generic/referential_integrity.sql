{% test referential_integrity(model, column_name, ref_model, ref_column) %}

SELECT
    {{ column_name }}
FROM {{ model }} m
LEFT JOIN {{ ref(ref_model) }} r
    ON m.{{ column_name }} = r.{{ ref_column }}
WHERE r.{{ ref_column }} IS NULL

{% endtest %}