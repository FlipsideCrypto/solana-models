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
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    program_id,
    signers,
    account_keys,
    instruction,
    inner_instruction,
    pre_balances,
    post_balances,
    pre_token_balances,
    post_token_balances,
    coalesce(
    staking_lp_actions_2_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id', 'tx_id', 'index', 'inner_index']
        ) }}
    ) AS fact_staking_lp_actions_id,
    coalesce(inserted_timestamp,'2000-01-01') AS inserted_timestamp,
    coalesce(modified_timestamp, '2000-01-01') AS modified_timestamp
FROM
    {{ ref('silver__staking_lp_actions_2') }}
