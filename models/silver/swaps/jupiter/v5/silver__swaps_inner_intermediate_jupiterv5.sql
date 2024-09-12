{{ config(
    materialized = 'table',
    unique_key = ['swaps_inner_intermediate_jupiterv5_id'],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(tx_id, swapper, from_mint, to_mint)'
    )
) }}

WITH base AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        program_id,
        INDEX,
        inner_index,
        log_index,
        succeeded,
        event_type,
        decoded_log :args :amm :: STRING AS swap_program_id,
        decoded_log :args :inputMint :: STRING AS from_mint,
        decoded_log :args :inputAmount :: STRING AS from_amount,
        decoded_log :args :outputMint :: STRING AS to_mint,
        decoded_log :args :outputAmount :: STRING AS to_amount,
        _inserted_timestamp,
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        program_id IN (
            'JUP5pEAZeHdHrLxh5UCwAbpjGwYKKoquCpda2hfP4u8',
            'JUP5cHjnnCx2DppVsufsLrXs8EBZeEZzGtEK9Gdz6ow'
        )
        AND event_type = 'SwapEvent'
        AND succeeded
        AND _inserted_timestamp :: DATE BETWEEN '2024-09-06'
        AND '2024-09-07'
),
swappers AS (
    SELECT
        tx_id,
        INDEX,
        inner_index,
        silver.udf_get_account_pubkey_by_name(
            'userTransferAuthority',
            decoded_instruction :accounts
        ) AS swapper,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        _inserted_timestamp :: DATE = '2023-11-03'
        AND program_id IN (
            'JUP5cHjnnCx2DppVsufsLrXs8EBZeEZzGtEK9Gdz6ow',
            'JUP5pEAZeHdHrLxh5UCwAbpjGwYKKoquCpda2hfP4u8'
        )
        AND event_type = 'route'
        AND swapper IS NOT NULL
),
token_decimals AS (
    SELECT
        mint,
        DECIMAL
    FROM
        {{ ref('silver__decoded_metadata') }}
    UNION ALL
    SELECT
        'So11111111111111111111111111111111111111112',
        9
    UNION ALL
    SELECT
        'GyD5AvrcZAhSP5rrhXXGPUHri6sbkRpq67xfG3x8ourT',
        9
),
pre_final AS (
    SELECT
        b.block_timestamp,
        b.block_id,
        b.tx_id,
        b.index,
        b.inner_index,
        b.log_index,
        ROW_NUMBER() over (
            PARTITION BY b.tx_id,
            b.index
            ORDER BY
                b.log_index
        ) -1 AS swap_index,
        b.succeeded,
        b.swap_program_id,
        b.program_id AS aggregator_program_id,
        s.swapper,
        b.from_mint,
        b.from_amount AS from_amount_int,
        b.from_amount * pow(
            10,- d.decimal
        ) AS from_amount,
        b.to_mint,
        b.to_amount AS to_amount_int,
        b.to_amount * pow(
            10,- d2.decimal
        ) AS to_amount,
        b._inserted_timestamp
    FROM
        base b
        LEFT OUTER JOIN swappers s
        ON b.tx_id = s.tx_id
        AND b.index = s.index
        LEFT OUTER JOIN token_decimals d
        ON b.from_mint = d.mint
        LEFT OUTER JOIN token_decimals d2
        ON b.to_mint = d2.mint
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    INDEX,
    inner_index,
    log_index,
    swap_index,
    succeeded,
    swap_program_id,
    aggregator_program_id,
    swapper,
    from_mint,
    from_amount,
    to_mint,
    to_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','log_index']) }} AS swaps_inner_intermediate_jupiterv5_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
