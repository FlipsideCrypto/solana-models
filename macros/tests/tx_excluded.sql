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
        {{  "'" + excluded_tx_ids | join("','") + "'"}}
    )
{% endtest %}
