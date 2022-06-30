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
), 

balance_adjust_tx as (
  SELECT 
      tx_id
  FROM {{ ref('silver__staking_lp_actions') }}

  {% if is_incremental() %}
     WHERE _inserted_timestamp::date >= current_date - 1
  {% endif %}

  GROUP BY tx_id
  HAVING count(tx_id) > 1
),    

balance_adjust_index AS (
    SELECT 
        a.tx_id,  
        index, 
        event_type, 
        pre_staked_balance, 
        post_staked_balance
    FROM balance_adjust_tx a
    
    INNER JOIN tx_base b 
    ON a.tx_id = b.tx_id 
    
    WHERE event_type = 'split' OR event_type = 'initialize'
),  

new_bal AS (
  SELECT 
    b.tx_id, 
    b.index, 
    b.event_type, 
    CASE WHEN b.index > ai.index THEN
        ai.post_staked_balance
    WHEN ai.event_type = 'initialize' THEN
        0
    ELSE 
        b.pre_staked_balance 
    END AS pre_staked_balance, 
    CASE WHEN b.event_type = 'deactivate' THEN 
        0
    WHEN ai.event_type = 'initialize' THEN
        0
    ELSE
        b.post_staked_balance
    END AS post_staked_balance 
  FROM tx_base b
  
  LEFT OUTER JOIN balance_adjust_index ai 
  ON b.tx_id = ai.tx_id  
  
  WHERE 
    (ai.event_type = 'split' 
  OR ai.event_type = 'initialize')
  AND b.tx_id IN (
    SELECT 
        tx_id 
    FROM balance_adjust_tx
  )
)

  SELECT 
      block_id, 
      block_timestamp, 
      b.tx_id,
      succeeded, 
      b.index, 
      b.event_type, 
      signers, 
      stake_authority,
      stake_account,
      n.pre_staked_balance, 
      n.post_staked_balance,
      vote_account, 
      node_pubkey,
      validator_rank, 
      commission, 
      COALESCE(
        label, 
        vote_account
      ) AS validator_name
FROM remove_nulls b

LEFT OUTER JOIN new_bal n 
ON b.tx_id = n.tx_id 
AND b.index = n.index
AND b.event_type = n.event_type

LEFT OUTER JOIN validators v
ON vote_account = vote_pubkey

LEFT OUTER JOIN {{ ref('core__dim_labels') }}
ON vote_account = address