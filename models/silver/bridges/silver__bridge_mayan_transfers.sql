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
        program_id =
            '8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E'


{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-09-19'
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
    A.block_timestamp :: DATE >= '2022-09-19'
{% endif %}
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
)
pre_final AS (

    SELECT
        *
    FROM
        inbound_mayan
    UNION ALL
    SELECT
        *
    FROM
        outbound_mayan

)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','tx_id', 'index']
    ) }} AS bridge_mayan_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
