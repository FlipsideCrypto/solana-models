{% test tx_excluded(
        model,
        excluded_tx_ids
    ) %}

SELECT
    *
FROM
    {{ model }}
WHERE
    tx_id IN(
        SELECT
            DISTINCT tx_id
        FROM
            {{ excluded_tx_ids }}

    )
{% endtest %}
