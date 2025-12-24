{% test record_count_reconciliation(model, model_a,model_b) %}

WITH model_a_ct AS (
    SELECT COUNT(*) AS cnt FROM {{ ref(model_a) }}
),
model_b_ct AS (
    SELECT COUNT(*) AS cnt FROM {{ ref(model_b) }}
)
SELECT *
FROM model_a_ct, model_b_ct
WHERE ABS(model_a_ct.cnt - model_b_ct.cnt) > 0

{% endtest %}
