
{{ config(
    materialized = 'view',
    tags = ['daily']
) }}

WITH verified_stablecoins AS (
    SELECT
        token_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        token_address IS NOT NULL
),

burns AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        'Burn' as event_name,
        mint,
        burn_amount as amount_raw,
        burn_amount / pow(10,decimal) as amount,
        token_account,
        decimal,
        _inserted_timestamp,
        token_burn_actions_id as stablecoins_mint_burn_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        {{ ref('silver__token_burn_actions') }} A
        INNER JOIN verified_stablecoins b
        ON A.mint = b.token_address
    WHERE 
        a.block_timestamp::date >= '2025-06-01'
),

mints AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        'Mint' as event_name,
        mint,
        mint_amount as amount_raw,
        mint_amount / pow(10,decimal) as amount,
        token_account,
        decimal,
        _inserted_timestamp,
        token_mint_actions_id as stablecoins_mint_burn_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        {{ ref('silver__token_mint_actions') }} A
        INNER JOIN verified_stablecoins b
        ON A.mint = b.token_address
    WHERE 
        a.block_timestamp::date >= '2025-06-01'
),

all_mint_burn AS (
    SELECT * FROM burns
    UNION ALL
    SELECT * FROM mints
)

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_name,
    mint,
    amount_raw,
    amount,
    token_account,
    decimal,
    _inserted_timestamp,
    stablecoins_mint_burn_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM all_mint_burn


