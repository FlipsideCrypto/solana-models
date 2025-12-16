
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


all_transfers AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        tx_from,
        tx_to,
        mint,
        amount,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        {{ ref('silver__transfers') }} a
        INNER JOIN verified_stablecoins b on a.mint = b.token_address
    WHERE
        block_timestamp::date >= '2025-06-01'
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    tx_from,
    tx_to,
    mint,
    amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id', 'index']) }} AS stablecoins_transfers_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    all_transfers
