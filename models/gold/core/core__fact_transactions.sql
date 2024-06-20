{{ config(
    materialized = 'view',
    post_hook = 'ALTER VIEW {{this}} SET CHANGE_TRACKING = TRUE;',
    tags = ['scheduled_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    recent_block_hash,
    signers,
    fee,
    succeeded,
    account_keys,
    pre_balances,
    post_balances,
    pre_token_balances,
    post_token_balances,
    instructions,
    inner_instructions,
    log_messages,
    units_consumed,
    units_limit,
    tx_size,
    COALESCE (
        transactions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_transactions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__transactions') }}
