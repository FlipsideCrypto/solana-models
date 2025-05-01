{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }}},
    unique_key = ['fact_rewards_staking_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(stake_pubkey, epoch_earned)'),
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

{% set switchover_block_id = 292334107 %}

SELECT
    block_timestamp,
    block_id,
    stake_pubkey,
    epoch_earned,
    reward_amount_sol,
    post_balance_sol,
    rewards_staking_2_id AS fact_rewards_staking_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    epoch_id AS dim_epoch_id
FROM
    {{ ref('silver__rewards_staking_2') }}
WHERE
    block_id > {{ switchover_block_id }}
    {% if is_incremental() %}
    AND modified_timestamp >= '{{ max_modified_timestamp }}'
    {% endif %}
{% if not is_incremental() %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    stake_pubkey,
    epoch_earned,
    reward_amount_sol,
    post_balance_sol,
    rewards_staking_id AS fact_rewards_staking_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    epoch_id AS dim_epoch_id
FROM
    {{ ref('silver__rewards_staking_view') }}
WHERE
    block_id <= {{ switchover_block_id }}
{% endif %}
