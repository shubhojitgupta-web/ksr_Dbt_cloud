{% test record_count_reconciliation(
    source_model,
    target_model,
    expected_diff=0
) %}

WITH source_count AS (
    SELECT COUNT(*) AS cnt FROM {{ ref(source_model) }}
),
target_count AS (
    SELECT COUNT(*) AS cnt FROM {{ ref(target_model) }}
)

SELECT
    source_count.cnt AS source_count,
    target_count.cnt AS target_count,
    (source_count.cnt - target_count.cnt) AS diff
FROM source_count
CROSS JOIN target_count
WHERE ABS(source_count.cnt - target_count.cnt) != {{ expected_diff }}

{% endtest %}
