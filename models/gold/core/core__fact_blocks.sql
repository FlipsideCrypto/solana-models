{{ config(
    materialized = 'incremental',
    unique_key = ['block_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(block_id)'),
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
    block_id,
    block_timestamp, 
    network,
    chain_id,
    block_height, 
    block_hash, 
    previous_block_id, 
    previous_block_hash,
    COALESCE (
        blocks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id']
        ) }}
    ) AS fact_blocks_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    {{ ref('silver__blocks') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}