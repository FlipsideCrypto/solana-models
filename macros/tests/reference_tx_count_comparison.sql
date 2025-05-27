{% test reference_tx_count_comparison(model, reference_table, id_column='tx_id') %}
    {% set failures %}
        WITH reference_count AS (
            SELECT
                COUNT({{ id_column }}) AS reference_count
            FROM {{ ref(reference_table) }}
            WHERE block_timestamp BETWEEN current_date - 2
                                      AND current_timestamp() - INTERVAL '2 HOUR'
        ),
        model_count AS (
            SELECT
                COUNT({{ id_column }}) AS model_count
            FROM {{ model }}
            WHERE block_timestamp BETWEEN current_date - 2
                                      AND current_timestamp() - INTERVAL '2 HOUR'
        )
        SELECT
            COALESCE(r.reference_count, 0) AS reference_count,
            COALESCE(m.model_count, 0) AS model_count,
            reference_count - model_count AS count_difference
        FROM reference_count r
        FULL OUTER JOIN model_count m
        WHERE reference_count <> model_count
    {% endset %}

    SELECT *
    FROM ({{ failures }})
{% endtest %}

