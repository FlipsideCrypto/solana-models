{{ config (
    materialized = 'incremental',
    unique_key = 'program_id'
) }}

WITH base AS (

    SELECT
        SPLIT_PART(
            id,
            '-',
            3
        ) :: STRING AS program_id,
        MIN(block_id) AS min_decoded_block
    FROM
        {{ ref('streamline__complete_decoded_instructions') }}
    GROUP BY
        1
),
program_last_processed AS (
    SELECT
        b.*,
        bl.block_timestamp :: DATE AS min_decoded_block_timestamp_date
    FROM
        base b
        JOIN {{ ref('silver__blocks') }}
        bl
        ON bl.block_id = b.min_decoded_block
),
pre_final as (
    SELECT
        h.program_id,
        COALESCE(
            p.min_decoded_block_timestamp_date,
            h.default_backfill_start_block_timestamp
        ) :: DATE AS min_decoded_block_timestamp_date,
        COALESCE(
            p.min_decoded_block,
            h.default_backfill_start_block_id
        ) AS min_decoded_block_id,
        first_block_id
    FROM
        {{ ref('streamline__idls_history') }}
        h
        LEFT JOIN program_last_processed p
        ON p.program_id = h.program_id
    WHERE 
        min_decoded_block_id > first_block_id
)
select 
    e.block_id,
    e.block_timestamp,
    e.tx_id,
    e.index,
    e.program_id,
    e.instruction
from {{ ref('silver__events') }} e 
join pre_final pf on 
    e.program_id = pf.program_id 
    and e.block_timestamp::date <= pf.min_decoded_block_timestamp_date
    and e.block_id >= pf.first_block_id
where 
    pf.program_id not in (select distinct(program_id) from {{ ref('streamline__complete_decoded_history') }})
{% if is_incremental() %}
or 
    pf.program_id not in (select distinct(program_id) from {{ this }})
{% endif %}