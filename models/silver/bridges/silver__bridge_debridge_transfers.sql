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
        (
            program_id IN (
                'dst5MGcFPoBeREFAA5E3tU5ij8m5uVYwkzkSAbsLbNo',
                -- debridge in
                'src5qyZHqTqecJV4aY6Cb6zDZLMDzrDKKezs22MPHr4' -- debridge out
            )
            OR (
                program_id = 'DEbrdGj3HsRsAzx6uH4MKyREKxVAfBydijLUF3ygsFfh'
                AND ARRAY_CONTAINS(
                    'src5qyZHqTqecJV4aY6Cb6zDZLMDzrDKKezs22MPHr4' :: variant,
                    inner_instruction_program_ids
                )
            )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-12-14'
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
    WHERE
        b.program_id IN (
            'src5qyZHqTqecJV4aY6Cb6zDZLMDzrDKKezs22MPHr4',
            'dst5MGcFPoBeREFAA5E3tU5ij8m5uVYwkzkSAbsLbNo'
        )

{% if is_incremental() %}
and
    A._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
and
    A.block_timestamp :: DATE >= '2022-12-14'
{% endif %}
),
inbound AS (
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
outbound AS (
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
        tx_to AS temp_receiver,
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
        AND amount != 0.01735944
        AND b.program_id <> '11111111111111111111111111111111'
        qualify ROW_NUMBER() over (
            PARTITION BY A.tx_id
            ORDER BY
                A.index
        ) = 1
),
cancel_bridging AS ( -- bridging can be cancelled after initial outbound tx
    SELECT
        block_timestamp,
        tx_id,
        instruction :accounts [28] :: STRING AS returner,
        instruction :accounts [30] :: STRING AS canceler
    FROM
        base_events
    WHERE
        program_id = 'DEbrdGj3HsRsAzx6uH4MKyREKxVAfBydijLUF3ygsFfh'
),
outbound_fulfilled AS ( 
    SELECT
        A.*
    FROM
        outbound A
        LEFT JOIN cancel_bridging b
        ON A.user_address = b.canceler
        AND A.temp_receiver = b.returner
        and a.block_timestamp < b.block_timestamp
    WHERE
        b.tx_id IS NULL

)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    program_id,
    platform,
    direction,
    user_address,
    amount,
    mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','tx_id', 'index']
    ) }} AS bridge_debridge_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    outbound_fulfilled
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    program_id,
    platform,
    direction,
    user_address,
    amount,
    mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','tx_id', 'index']
    ) }} AS bridge_debridge_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    inbound
