{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} },
    unique_key = ['ez_staking_lp_actions_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','ROUND(block_id, -3)','event_type'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, stake_account, vote_account, node_pubkey)'),
    full_refresh = false,
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
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    signers,
    stake_authority,
    withdraw_authority,
    stake_account,
    stake_active,
    pre_tx_staked_balance,
    post_tx_staked_balance,
    withdraw_amount,
    withdraw_destination,
    move_amount,
    move_destination,
    vote_account,
    node_pubkey,
    validator_rank,
    commission,
    validator_name,
    coalesce(
        staking_lp_actions_labeled_2_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id', 'tx_id', 'index']
        ) }}
    ) AS ez_staking_lp_actions_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp
FROM
    {{ ref('silver__staking_lp_actions_labeled_2') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}