{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
    unique_key = ['fact_gauges_votes_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id)'),
    tags = ['scheduled_non_core']
) }}

SELECT
    'saber' AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    NULL AS voter_nft,
    gauge,
    power,
    delegated_shares,
    COALESCE (
        gauges_votes_saber_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','voter','gauge']
        ) }}
    ) AS fact_gauges_votes_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__gauges_votes_saber') }}
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
{% if not is_incremental() %}
UNION
SELECT
    'marinade' AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_nft,
    gauge,
    NULL AS power,
    delegated_shares,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'voter', 'voter_nft', 'gauge']
    ) }} AS fact_gauges_votes_id,
    '2000-01-01' AS inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__gauges_votes_marinade_view') }}
{% endif %}
