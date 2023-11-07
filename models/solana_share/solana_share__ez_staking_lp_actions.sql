{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

  SELECT 
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    INDEX,
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
    validator_name
FROM {{ref('gov__ez_staking_lp_actions')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'