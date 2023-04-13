{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

WITH base_events AS(

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id IN (
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h',
            '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-03-06'
{% endif %}
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    instruction :accounts [3] :: STRING AS liquidity_pool,
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' AS owner,
    instruction :accounts [4] :: STRING AS mint_authority,
    instruction :accounts [9] :: STRING AS token_a_account,
    instruction :accounts [10] :: STRING AS token_b_account,
    instruction :accounts [6] :: STRING AS pool_token,
    _inserted_timestamp
FROM
    base_events e
WHERE
    program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
    AND event_type IS NULL
    AND succeeded
    AND instruction :accounts [0] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
    AND instruction :accounts [1] = '11111111111111111111111111111111'
    AND instruction :accounts [2] = 'SysvarRent111111111111111111111111111111111'
    AND instruction :accounts [4] = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'
    AND instruction :accounts [8] <> '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'
UNION
SELECT
    block_timestamp,
    block_id,
    tx_id,
    instruction :accounts [1] :: STRING AS liquidity_pool,
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' AS owner,
    instruction :accounts [2] :: STRING AS mint_authority,
    instruction :accounts [7] :: STRING AS token_a_account,
    instruction :accounts [8] :: STRING AS token_b_account,
    instruction :accounts [4] :: STRING AS pool_token,
    _inserted_timestamp
FROM
    base_events e
WHERE
    program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
    AND event_type IS NULL
    AND succeeded
    AND instruction :accounts [0] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
    AND instruction :accounts [12] = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'
UNION
SELECT
    block_timestamp,
    block_id,
    tx_id,
    instruction :accounts [3] :: STRING AS liquidity_pool,
    '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h' AS owner,
    instruction :accounts [4] :: STRING AS mint_authority,
    instruction :accounts [9] :: STRING AS token_a_account,
    instruction :accounts [10] :: STRING AS token_b_account,
    instruction :accounts [6] :: STRING AS pool_token,
    _inserted_timestamp
FROM
    base_events e
WHERE
    program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
    AND event_type IS NULL
    AND succeeded
    AND instruction :accounts [0] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
    AND instruction :accounts [1] = '11111111111111111111111111111111'
    AND instruction :accounts [2] = 'SysvarRent111111111111111111111111111111111'
UNION
SELECT
    block_timestamp,
    block_id,
    tx_id,
    instruction :accounts [1] :: STRING AS liquidity_pool,
    '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv' AS owner,
    instruction :accounts [2] :: STRING AS mint_authority,
    instruction :accounts [7] :: STRING AS token_a_account,
    instruction :accounts [8] :: STRING AS token_b_account,
    instruction :accounts [4] :: STRING AS pool_token,
    _inserted_timestamp
FROM
    base_events e
WHERE
    program_id = '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
    AND event_type IS NULL
    AND succeeded
    AND instruction :accounts [0] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
    AND instruction :accounts [13] = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'
SELECT
    block_timestamp,
    block_id,
    tx_id,
    instruction :accounts [4] :: STRING AS liquidity_pool,
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' AS owner,
    instruction :accounts [5] :: STRING AS mint_authority,
    instruction :accounts [8] :: STRING AS token_a_account,
    instruction :accounts [9] :: STRING AS token_b_account,
    instruction :accounts [7] :: STRING AS pool_token,
    _inserted_timestamp
FROM
    base_events
WHERE
    program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
    AND event_type IS NULL
    AND succeeded
    AND instruction :accounts [0] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
    AND instruction :accounts [1] = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
    AND instruction :accounts [15] = 'srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX'
    AND block_timestamp :: DATE > '2023-01-15';
