{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
    unique_key = ['fact_proposal_votes_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id)'),
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
    'realms' AS governance_platform,
    program_id AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_account,
    NULL AS voter_nft,
    proposal,
    realms_id,
    vote_choice,
    vote_rank,
    vote_weight,
  COALESCE (
    proposal_votes_realms_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','index']
        ) }}
    ) AS fact_proposal_votes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__proposal_votes_realms') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
{% if not is_incremental() %}
UNION ALL
SELECT
    'tribeca' AS governance_platform,
    'marinade' AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_account,
    voter_nft,
    proposal,
    NULL AS realms_id,
    NULL AS vote_choice,
    NULL AS vote_rank,
    NULL AS vote_weight,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'voter_nft', 'proposal']
    ) }} AS fact_proposal_votes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__proposal_votes_marinade_view') }}
{% endif %}

