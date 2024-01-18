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
        solana.silver.events
    WHERE
        (program_id =
            'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
        or ARRAY_CONTAINS(
                'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb' :: variant,
                inner_instruction_program_ids
            ))
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
)

,

wormhole_events as (
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signers,
    index,
    null as inner_index,
    program_id,
    'outer worm' as program_test,
    instruction,
    _inserted_timestamp
from base_events
WHERE
program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
union all
select
    e.block_timestamp,
    e.block_id,
    e.tx_id,
    e.succeeded,
    e.signers,
    e.index,
    i.index AS inner_index,
    i.value :programId :: STRING AS program_id,
    program_id as program_test,
    i.value as instruction,
    e._inserted_timestamp
FROM
base_events e,
TABLE(FLATTEN(e.inner_instruction :instructions)) i
WHERE
e.program_id <> 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
AND i.value :programId :: STRING = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'

)
-- select * from wormhole_event;

,
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
        solana.silver.transfers A
        INNER JOIN base_events b
        ON A.tx_id = b.tx_id


{% if is_incremental() %}
where a._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    where a.block_timestamp :: DATE >= '2021-09-13'
{% endif %}
)

-- select * from base_transfers;

,
wormhole_1 AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.index,
        A.program_id,
    a.program_test,
        'wormhole' AS platform,
        CASE
            WHEN b.tx_from = 'GugU1tP7doLeTw9hQP51xRJyS8Da1fWxuiy2rVrnMD2m' THEN 'inbound'
            ELSE 'outbound'
        END AS direction,
        A.signers [0] :: STRING AS user_address,
        b.mint,
        A._inserted_timestamp,
        'gug' as test,
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
        AND a.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
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
        11,
        12)
        
  -- select * from wormhole_1;    
        
        ,

inbound as (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        b.index,
        b.program_id,
    b.program_test,
        'wormhole' AS platform,
        'inbound' AS direction,
        b.signers [0] :: STRING AS user_address,
        A.mint,
        A._inserted_timestamp,
        'inb' as test,
        A.mint_amount / pow(
            10,
            A.decimal
        ) AS amount
    FROM
        solana.silver.token_mint_actions A
        INNER JOIN wormhole_events b USING(tx_id)
    WHERE
        succeeded
        AND A.mint_amount > 0
            and ((b.inner_index is null
        AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb')
        or (b.inner_index is not null
        AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb')
)
        AND b.instruction :accounts [5] = A.token_account
        qualify ROW_NUMBER() over (
            PARTITION BY A.tx_id
            ORDER BY
                A.index,
                A.inner_index
        ) = 1
)

,
outbound as (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        b.index,
        b.program_id,
    b.program_test,
        'wormhole' AS platform,
        'outbound' AS direction,
        b.signers [0] :: STRING AS user_address,
        A.mint,
        A._inserted_timestamp,
        'outb' as test,
                A.burn_amount / pow(
            10,
            A.decimal
        ) AS amount
    FROM
        solana.silver.token_burn_actions A
        INNER JOIN wormhole_events b USING(tx_id)
    WHERE
        succeeded
        AND A.burn_amount > 0
                and ((b.inner_index is null
        AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb')
        or (b.inner_index is not null
        AND b.program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb')
)
        qualify ROW_NUMBER() over (
            PARTITION BY A.tx_id
            ORDER BY
                A.index,
                A.inner_index
        ) = 1
)
-- select * from outbound;,
,
fin as (
    SELECT
        *
    FROM
        wormhole_1
    union all
    SELECT
        *
    FROM
        inbound
    union all
    SELECT
        *
    FROM
        outbound)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','tx_id', 'index']
    ) }} AS bridge_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin


