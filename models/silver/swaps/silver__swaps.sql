{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","program_id"],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        program_id,
        case when program_id in ('Crt7UoUR6QgrFrN7j8rmSQpUTNWNSitSwWvsWGf1qZ5t','SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ') then 
            'saber'
        else 
            'jupiter'
        end as program_name,
        swapper,
        from_mint,
        from_amt,
        to_mint,
        to_amt,
        swap_index,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__swaps_intermediate_generic') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    'raydium' as program_name,
    swapper,
    from_mint,
    from_amt,
    to_mint,
    to_amt,
    swap_index,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_intermediate_raydium') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    'orca' as program_name,
    swapper,
    from_mint,
    from_amt,
    to_mint,
    to_amt,
    swap_index,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_intermediate_orca') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    'jupiter' as program_name,
    swapper,
    from_mint,
    from_amt,
    to_mint,
    to_amt,
    swap_index,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv4') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
base_swaps AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        program_id,
        program_name,
        swapper,
        from_mint,
        to_mint,
        _inserted_timestamp,
        MIN(_log_id) as _log_id,
        MIN(swap_index) AS swap_index,
        SUM(from_amt) AS from_amt,
        SUM(to_amt) AS to_amt
    FROM
        base
    WHERE
        from_amt IS NOT NULL
        AND to_amt IS NOT NULL
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
intermediate_swaps AS (
    SELECT
        *,
        MAX(swap_index) over (
            PARTITION BY tx_id
        ) AS max_swap_index
    FROM
        base_swaps
),
refunds AS (
    SELECT
        tx_id,
        to_mint,
        SUM(to_amt) AS to_amt
    FROM
        base
    WHERE
        from_amt IS NULL
        AND from_mint IS NULL
        AND to_amt IS NOT NULL
    GROUP BY
        1,
        2
),
fees AS (
    SELECT
        tx_id,
        from_mint,
        SUM(from_amt) AS from_amt
    FROM
        base
    WHERE
        to_amt IS NULL
        AND to_mint IS NULL
        AND from_amt IS NOT NULL
    GROUP BY
        1,
        2
),
pre_final AS (
    SELECT
        b1.block_id,
        b1.block_timestamp,
        b1.program_id,
        b1.tx_id,
        b1.succeeded,
        b1.swapper,
        b1.from_amt,
        b1.from_mint,
        COALESCE(
            b2.to_amt,
            b1.to_amt
        ) AS to_amt,
        COALESCE(
            b2.to_mint,
            b1.to_mint
        ) AS to_mint,
        b1._log_id,
        b1._inserted_timestamp
    FROM
        intermediate_swaps b1
        LEFT OUTER JOIN intermediate_swaps b2
        ON b2.tx_id = b1.tx_id
        AND b2.swap_index <> b1.swap_index
        AND b2.swap_index > 1
        AND b1.program_name = b2.program_name
    WHERE
        b1.swap_index = 1
        AND (
            b2.swap_index = b2.max_swap_index
            OR b2.tx_id IS NULL
        )
)
SELECT
    pf.block_id,
    pf.block_timestamp,
    pf.program_id,
    pf.tx_id,
    pf.succeeded,
    pf.swapper,
    pf.from_amt - COALESCE(
        r.to_amt,
        0
    ) + COALESCE(
        f.from_amt,
        0
    ) AS from_amt,
    pf.from_mint,
    pf.to_amt - COALESCE(
        f2.from_amt,
        0
    ) AS to_amt,
    pf.to_mint,
    pf._log_id,
    pf._inserted_timestamp
FROM
    pre_final pf
    LEFT OUTER JOIN refunds r
    ON r.tx_id = pf.tx_id
    AND r.to_mint = pf.from_mint
    LEFT OUTER JOIN fees f
    ON f.tx_id = pf.tx_id
    AND f.from_mint = pf.from_mint
    LEFT OUTER JOIN fees f2
    ON f2.tx_id = pf.tx_id
    AND f2.from_mint = pf.to_mint
WHERE
    pf.succeeded
UNION
SELECT
    pf.block_id,
    pf.block_timestamp,
    pf.program_id,
    pf.tx_id,
    pf.succeeded,
    pf.swapper,
    0 AS from_amt,
    pf.from_mint,
    0 AS to_amt,
    pf.to_mint,
    pf._log_id,
    pf._inserted_timestamp
FROM
    pre_final pf
WHERE
    succeeded = FALSE qualify(ROW_NUMBER() over (PARTITION BY block_id, tx_id, program_id, from_mint, to_mint
ORDER BY
    block_timestamp)) = 1
UNION
SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint,
    NULL as _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6') }}
{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

