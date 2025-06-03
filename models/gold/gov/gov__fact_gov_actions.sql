{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
    unique_key = ['fact_gov_actions_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id)'),
    tags = ['scheduled_non_core']
) }}

{% if execute %}

    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(modified_timestamp) AS max_modified_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

-- Only select from the deprecated model during the initial FR
{% if not is_incremental() %}
SELECT 
    'saber' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signer,
    locker_account,
    null as locker_nft,
    mint,
    action,
    amount,
    COALESCE (
        gov_actions_saber_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_gov_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__gov_actions_saber_view') }}
UNION ALL
{% endif %}
-- Only select from active models during incremental
SELECT 
    'marinade' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signer,
    locker_account,
    locker_nft,
    mint,
    action,
    amount,
    COALESCE (
        gov_actions_marinade_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_gov_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__gov_actions_marinade') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}