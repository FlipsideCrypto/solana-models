{{ config(
    materialized = 'view',
    meta = {
        'database_tags': {
            'table': {
                'PURPOSE': 'STAKING'
            }
        }
    },
    tags = ['scheduled_non_core']
) }}

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
    coalesce(inserted_timestamp, '2000-01-01') AS inserted_timestamp,
    coalesce(modified_timestamp, '2000-01-01') AS modified_timestamp
FROM
    {{ ref('silver__staking_lp_actions_labeled_2') }}