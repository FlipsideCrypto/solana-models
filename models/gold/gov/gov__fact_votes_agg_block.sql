{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
    unique_key = ['fact_votes_agg_block_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(block_id)'),
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT 
              max(modified_timestamp) AS max_modified_timestamp
            FROM 
              {{ this }}
        {% endset %}
        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

SELECT 
    block_timestamp,
    block_id,
    num_votes,
    COALESCE (
        votes_agg_block_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id']
        ) }}
    ) AS fact_votes_agg_block_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__votes_agg_block') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
