{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH tx_base AS (
  SELECT 
      block_id, 
      block_timestamp, 
      tx_id, 
      succeeded, 
      index, 
      event_type, 
      signers, 
      signers[0] :: STRING AS stake_authority, 
      COALESCE(
        signers[1] :: STRING, 
        instruction:parsed:info:stakeAccount :: STRING
      ) AS stake_acct, 
      pre_balances[1] :: INTEGER AS pre_staked_balance, 
      post_balances[1] :: INTEGER AS post_staked_balance, 
      instruction:parsed:info:voteAccount :: STRING AS vote_acct
  FROM {{ ref('silver__staking_lp_actions') }}

  {% if is_incremental() %}
     WHERE _inserted_timestamp::date >= current_date - 1
  {% endif %}

), 

validators AS (
    SELECT 
        value:nodePubkey :: STRING AS node_pubkey, 
        value:commission :: INTEGER AS commission, 
        value:votePubkey :: STRING AS vote_pubkey, 
        rank() OVER (ORDER by value:activatedStake :: INTEGER desc) as validator_rank
    FROM 
     {{ source(
      'solana_external',
      'validator_metadata_api'
    ) }} 
    
), 

remove_nulls AS (
    SELECT 
        block_id, 
        block_timestamp, 
        tx_id,
        succeeded, 
        index, 
        event_type, 
        signers, 
        stake_authority,
        CASE WHEN stake_acct IS NULL THEN 
            LAST_VALUE(stake_acct) IGNORE NULLS OVER (PARTITION BY signers[0] :: STRING ORDER BY block_timestamp) 
        ELSE 
            stake_acct
        END AS stake_account, 
        pre_staked_balance, 
        post_staked_balance,
        CASE WHEN vote_acct IS NULL THEN 
            LAST_VALUE(vote_acct) IGNORE NULLS OVER (PARTITION BY signers[0] :: STRING ORDER BY block_timestamp) 
        ELSE 
            vote_acct
        END AS vote_account 
    FROM tx_base
)

SELECT 
    block_id, 
    block_timestamp, 
    tx_id,
    succeeded, 
    index, 
    event_type, 
    signers, 
    stake_authority,
    stake_account, 
    pre_staked_balance, 
    post_staked_balance,
    vote_account, 
    node_pubkey,
    validator_rank, 
    commission 
FROM remove_nulls 

LEFT OUTER JOIN validators v
ON vote_account = vote_pubkey