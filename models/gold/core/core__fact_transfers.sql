{{ config(
    materialized = 'incremental',
    unique_key = ['fact_transfers_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','ROUND(block_id, -3)'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, tx_from, tx_to, mint)'),
    full_refresh = false,
    tags = ['scheduled_core']
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_modified_query %}
    SELECT
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query)[0][0] %}
    {% endif %}
{% endif %}

SELECT 
    block_timestamp,
    block_id,
    tx_id,
    index,
    tx_from,
    tx_to,
    amount,
    mint,
    transfers_id AS fact_transfers_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    {{ ref('silver__transfers') }}
WHERE 
    succeeded

{% if is_incremental() %}
AND
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
