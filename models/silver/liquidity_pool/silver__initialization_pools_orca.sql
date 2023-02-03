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
            'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
            '11111111111111111111111111111111'
        )
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-02-14'
{% endif %}
),
orca_pool_creation AS(
    SELECT
        *,
        instruction :parsed :info :newAccount :: STRING AS liquidity_pool,
        instruction :parsed :info :owner :: STRING AS owner
    FROM
        base_events
    WHERE
        event_type = 'createAccount'
        AND owner IN (
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
            '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
        )
        AND program_id = '11111111111111111111111111111111'
        AND succeeded
),
orca_pools AS (
    SELECT
        l.block_timestamp,
        l.block_id,
        l.tx_id,
        l.liquidity_pool,
        l.owner,
        e.instruction :accounts [1] :: STRING AS mint_authority,
        e.instruction :accounts [2] :: STRING AS token_a_account,
        e.instruction :accounts [3] :: STRING AS token_b_account,
        e.instruction :accounts [4] :: STRING AS pool_token,
        l._inserted_timestamp
    FROM
        orca_pool_creation l
        LEFT JOIN base_events e
    WHERE
        l.tx_id = e.tx_id
        AND l.index <> e.index
),
whirlpools AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        instruction :accounts [4] :: STRING AS liquidity_pool,
        'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' AS owner,
        liquidity_pool AS mint_authority,
        instruction :accounts [5] :: STRING AS token_a_account,
        instruction :accounts [6] :: STRING AS token_b_account,
        NULL AS pool_token,
        _inserted_timestamp
    FROM
        base_events e
    WHERE
        program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
        AND event_type IS NULL
        AND succeeded
        AND instruction :accounts [8] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND instruction :accounts [9] = '11111111111111111111111111111111'
        AND instruction :accounts [10] = 'SysvarRent111111111111111111111111111111111'
)
SELECT
    *
FROM
    whirlpools
UNION
SELECT
    *
FROM
    orca_pools
