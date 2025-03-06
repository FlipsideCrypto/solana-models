{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","index","direction"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id,user_address,mint)'),
    tags = ['scheduled_non_core']
) }}

WITH base_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
            OR (
                ARRAY_CONTAINS(
                    'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb' :: variant,
                    inner_instruction_program_ids
                )
                AND program_id <> '8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E' -- exclude mayan bridge tx's
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
    AND block_timestamp :: DATE >= '2021-09-13'
{% endif %}
),
wormhole_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        signers,
        INDEX,
        NULL AS inner_index,
        program_id,
        instruction,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
    UNION ALL
    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        e.succeeded,
        e.signers,
        e.index,
        i.index AS inner_index,
        i.value :programId :: STRING AS program_id,
        i.value AS instruction,
        e._inserted_timestamp
    FROM
        base_events e,
        TABLE(FLATTEN(e.inner_instruction :instructions)) i
    WHERE
        e.program_id <> 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
        AND i.value :programId :: STRING = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
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
base_mint_actions AS (
    SELECT
        *
    FROM
        {{ ref('silver__token_mint_actions') }}
    WHERE event_type IN ('mintToChecked', 'mintTo')
{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
AND
    block_timestamp :: DATE >= '2021-09-13'
{% endif %}
),
base_burn_actions AS (
    SELECT
        *
    FROM
        {{ ref('silver__token_burn_actions') }}
{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    block_timestamp :: DATE >= '2021-09-13'
{% endif %}
),
wormhole_transfers AS (
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
        b.mint,
        A._inserted_timestamp,
        SUM(
            b.amount
        ) AS amount
    FROM
        wormhole_events A
        INNER JOIN base_transfers b USING(tx_id)
    WHERE
        b.amount > 0
        AND (
            b.tx_from = 'GugU1tP7doLeTw9hQP51xRJyS8Da1fWxuiy2rVrnMD2m'
            OR b.tx_to = 'GugU1tP7doLeTw9hQP51xRJyS8Da1fWxuiy2rVrnMD2m'
        )
        AND A.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
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
        10,
        11
),
inbound AS (
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
        A.mint,
        A._inserted_timestamp,
        A.mint_amount / pow(
            10,
            A.decimal
        ) AS amount
    FROM
        base_mint_actions A
        INNER JOIN wormhole_events b USING(tx_id)
    WHERE
        succeeded
        AND A.mint_amount > 0
        AND (
            (
                b.inner_index IS NULL
                AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
            )
            OR (
                b.inner_index IS NOT NULL
                AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
            )
        )
        AND b.instruction :accounts [5] = A.token_account
        qualify ROW_NUMBER() over (
            PARTITION BY A.tx_id
            ORDER BY
                A.index,
                A.inner_index
        ) = 1
),
outbound AS (
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
        A.mint,
        A._inserted_timestamp,
        A.burn_amount / pow(
            10,
            A.decimal
        ) AS amount
    FROM
        base_burn_actions A
        INNER JOIN wormhole_events b USING(tx_id)
    WHERE
        succeeded
        AND A.burn_amount > 0
        AND (
            (
                b.inner_index IS NULL
                AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
            )
            OR (
                b.inner_index IS NOT NULL
                AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
            )
        ) 
        AND EXISTS (
            SELECT 1
            FROM base_events e,
            TABLE(FLATTEN(e.inner_instruction:instructions)) i
            WHERE e.tx_id = A.tx_id
            AND e.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
            AND i.value:programId::string = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND i.value:parsed:type::string = 'burn'
            AND i.value:parsed:info:mint::string = A.mint)
        qualify ROW_NUMBER() over (
            PARTITION BY A.tx_id
            ORDER BY
                A.index,
                A.inner_index
        ) = 1
),
pre_final AS (
    SELECT
        *
    FROM
        wormhole_transfers
    UNION ALL
    SELECT
        *
    FROM
        inbound
    UNION ALL
    SELECT
        *
    FROM
        outbound
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','tx_id', 'index','direction']
    ) }} AS bridge_wormhole_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
