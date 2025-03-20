{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
    unique_key = ['fact_proposal_creation_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id)'),
    tags = ['scheduled_non_core']
) }}

SELECT
    'realms' AS governance_platform,
    program_id AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    realms_id,
    proposal,
    proposal_writer,
    proposal_name,
    vote_type,
    vote_options,
    COALESCE (
    proposal_creation_realms_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_proposal_creation_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__proposal_creation_realms') }}
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
