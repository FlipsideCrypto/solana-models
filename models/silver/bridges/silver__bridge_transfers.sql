{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","index"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH base_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id IN (
            'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb', --wormhole
            'dst5MGcFPoBeREFAA5E3tU5ij8m5uVYwkzkSAbsLbNo', -- debridge in
            'src5qyZHqTqecJV4aY6Cb6zDZLMDzrDKKezs22MPHr4', -- debridge out
            '8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E' -- mayan finance
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-09-13'
{% endif %}
),
base_transfers AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.succeeded,
        A.program_id,
        COALESCE(SPLIT_PART(A.index :: text, '.', 1) :: INT, A.index :: INT) AS INDEX,
        NULLIF(SPLIT_PART(A.index :: text, '.', 2), '') :: INT AS inner_index,
        A.tx_from,
        A.tx_to,
        A.amount,
        A.mint,
        b.program_id AS event_program_id,
        A._inserted_timestamp
    FROM
        {{ ref('silver__transfers') }} A
        INNER JOIN base_events b
        ON A.tx_id = b.tx_id

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    A.block_timestamp :: DATE >= '2021-09-13'
{% endif %}
),
inbound_debridge AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.index,
        A.program_id,
        'deBridge' AS platform,
        'inbound' AS direction,
        tx_to AS user_address,
        b.amount AS amount,
        b.mint,
        A._inserted_timestamp
    FROM
        base_events A
        LEFT JOIN base_transfers b
        ON A.tx_id = b.tx_id
        AND A.index = b.index
        AND A.signers [0] = A.instruction :accounts [1] :: STRING -- AND A.succeeded
    WHERE
        A.program_id = 'dst5MGcFPoBeREFAA5E3tU5ij8m5uVYwkzkSAbsLbNo'
        AND b.mint IS NOT NULL
        AND A.instruction :accounts [6] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
),
outbound_debridge AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.index,
        A.program_id,
        'deBridge' AS platform,
        'outbound' AS direction,
        tx_from AS user_address,
        b.amount AS amount,
        b.mint,
        A._inserted_timestamp
    FROM
        base_events A
        LEFT JOIN base_transfers b
        ON A.tx_id = b.tx_id
        AND A.index = b.index
    WHERE
        A.program_id = 'src5qyZHqTqecJV4aY6Cb6zDZLMDzrDKKezs22MPHr4'
        AND amount != 0.01735944 qualify ROW_NUMBER() over (
            PARTITION BY A.tx_id
            ORDER BY
                A.index
        ) = 1
),
outbound_mayan AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.index,
        A.program_id,
        'mayan finance' AS project,
        'outbound' AS direction,
        b.tx_from AS user_address,
        b.amount,
        b.mint,
        A._inserted_timestamp
    FROM
        base_events A
        LEFT JOIN base_transfers b
        ON A.tx_id = b.tx_id
        AND A.index = b.index
    WHERE
        b.tx_to = '5yZiE74sGLCT4uRoyeqz4iTYiUwX5uykiPRggCVih9PN'
        AND A.instruction :accounts [11] :: STRING = '11111111111111111111111111111111'
        AND A.program_id = '8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E'
),
inbound_mayan AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.index,
        A.program_id,
        'mayan finance' AS project,
        'inbound' AS direction,
        b.tx_to AS user_address,
        b.amount,
        b.mint,
        A._inserted_timestamp
    FROM
        base_events A
        LEFT JOIN base_transfers b
        ON A.tx_id = b.tx_id
        AND A.index = b.index
    WHERE
        b.tx_from = '5yZiE74sGLCT4uRoyeqz4iTYiUwX5uykiPRggCVih9PN'
        AND NOT b.tx_to = '7dm9am6Qx7cH64RB99Mzf7ZsLbEfmXM7ihXXCvMiT2X1'
        AND A.instruction :accounts [9] :: STRING = 'SysvarC1ock11111111111111111111111111111111'
        AND A.instruction :accounts [10] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND A.program_id = '8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E'
),
wormhole AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.index,
        A.program_id,
        'wormhole' AS platform,
        CASE
            WHEN b.tx_from = 'GugU1tP7doLeTw9hQP51xRJyS8Da1fWxuiy2rVrnMD2m' THEN 'inbound'
            ELSE 'outbound'
        END AS direction,
        A.signers [0] :: STRING AS user_address,
        SUM(
            b.amount
        ) AS amount,
        b.mint,
        A._inserted_timestamp
    FROM
        base_events A
        INNER JOIN base_transfers b USING(tx_id)
    WHERE
        b.amount > 0
        AND (
            b.tx_from = 'GugU1tP7doLeTw9hQP51xRJyS8Da1fWxuiy2rVrnMD2m'
            OR b.tx_to = 'GugU1tP7doLeTw9hQP51xRJyS8Da1fWxuiy2rVrnMD2m'
        )
        AND program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
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
        11,
        12
    UNION ALL
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        b.index,
        b.program_id,
        'wormhole' AS platform,
        'inbound' AS direction,
        b.signers [0] :: STRING AS user_address,
        A.mint_amount / pow(
            10,
            A.decimal
        ) AS amount,
        A.mint,
        A._inserted_timestamp
    FROM
        solana.silver.token_mint_actions A
        INNER JOIN base_events b USING(tx_id)
    WHERE
        succeeded
        AND A.mint_amount > 0
        AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
        AND b.instruction :accounts [5] = A.token_account qualify ROW_NUMBER() over (
            PARTITION BY A.tx_id
            ORDER BY
                A.index,
                A.inner_index
        ) = 1
    UNION ALL
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        b.index,
        b.program_id,
        'wormhole' AS platform,
        'outbound' AS direction,
        b.signers [0] :: STRING AS user_address,
        A.burn_amount / pow(
            10,
            A.decimal
        ) AS amount,
        A.mint,
        A._inserted_timestamp
    FROM
        solana.silver.token_burn_actions A
        INNER JOIN base_events b USING(tx_id)
    WHERE
        succeeded
        AND A.burn_amount > 0
        AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
),
pre_final AS (
    SELECT
        *
    FROM
        wormhole
    UNION ALL
    SELECT
        *
    FROM
        inbound_mayan
    UNION ALL
    SELECT
        *
    FROM
        outbound_mayan
    UNION ALL
    SELECT
        *
    FROM
        outbound_debridge
    UNION ALL
    SELECT
        *
    FROM
        inbound_debridge
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','tx_id', 'index']
    ) }} AS bridge_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
