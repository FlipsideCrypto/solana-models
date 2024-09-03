{% test reference_tx_missing(model, reference_tables, id_column='tx_id') %}
    {% set failures %}
        SELECT
            reference_table_name,
            {{ id_column }} AS missing_tx_id
        FROM (
            {% for reference_table in reference_tables %}
                SELECT
                    '{{ reference_table }}' AS reference_table_name,
                    {{ id_column }}
                FROM {{ ref(reference_table) }}
                WHERE {{ id_column }} NOT IN (SELECT {{ id_column }} FROM {{ model }} where modified_timestamp >= current_date - 7)
                AND _inserted_timestamp between current_date - 7 and current_timestamp() - INTERVAL '1 HOUR'
                {% if not loop.last %} UNION ALL {% endif %}
            {% endfor %}
        ) missing_tx_ids
    {% endset %}

    SELECT *
    FROM ({{ failures }})
{% endtest %}

