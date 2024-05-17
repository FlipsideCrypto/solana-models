 -- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = "swaps_inner_intermediate_jupiterv6_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core'],
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.swaps_inner_intermediate_jupiterv6__intermediate_tmp AS 
        SELECT 
            block_timestamp, 
            tx_id, 
            swapper, 
            swap_index,
            program_id,
        FROM
            {{ ref('silver__swaps_intermediate_jupiterv6') }}
        WHERE
        {% if is_incremental() %}
            _inserted_timestamp >= (
                SELECT
                    MAX(_inserted_timestamp) - INTERVAL '1 hour'
                FROM
                    {{ this }}
            )
        {% else %} 
            block_timestamp::date = '2024-05-01'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.swaps_inner_intermediate_jupiterv6__intermediate_tmp","block_timestamp::date") %}
{% endif %}

with base_swaps AS (
    SELECT
        *
    FROM
        silver.swaps_inner_intermediate_jupiterv6__intermediate_tmp
),
base_events AS (
    SELECT 
        block_timestamp,
        tx_id,
        index,
        inner_instruction_program_ids
    FROM 
        {{ ref('silver__events') }} e
    WHERE 
        {{ between_stmts }}
        AND program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
),
base_decoded AS (
    SELECT 
        block_timestamp,
        tx_id,
        index,
        decoded_instruction
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        {{ between_stmts }}
        AND program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
),
base_transfers AS (
    SELECT 
        block_timestamp,
        tx_id,
        index,
        decoded_instruction
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        {{ between_stmts }}
),
base AS (
    select 
        j.block_timestamp, 
        j.tx_id, 
        j.swapper, 
        j.swap_index, 
        d.decoded_instruction, 
        e.inner_instruction_program_ids
    from 
        {{ ref('silver__swaps_intermediate_jupiterv6') }} j
        join 
            {{ ref('silver__decoded_instructions_combined') }} d 
            on d.block_timestamp::date = j.block_timestamp::date
            and d.tx_id = j.tx_id 
            and d.index = j.swap_index 
            and d.program_id = j.program_id
        join 
            {{ ref('silver__events') }} e
            on d.block_timestamp::date = e.block_timestamp::date
            and d.tx_id = e.tx_id
            and d.index = e.index
    where 
        j.block_timestamp::date >= '2024-05-01'
        and e.block_timestamp::date >= '2024-05-01'
        and e.program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
),
transfers_modified_owners AS (
    select 
        t.tx_id,
        t.index,
        split_part(index,'.',1)::int as _index,
        split_part(index,'.',2)::int as _inner_index,
        solana.silver.udf_get_account_pubkey_by_name('programAuthority', decoded_instruction:accounts) AS acting_authority,
        case 
            when acting_authority is not null and tx_from = acting_authority then 
                b.swapper
            else 
                coalesce(ao_from.owner, t.tx_from)
        end as new_tx_from,
        case 
            when acting_authority is not null and tx_to = acting_authority then 
                b.swapper
            else 
                coalesce(ao_to.owner, t.tx_to)
        end as new_tx_to,
        case when new_tx_from = b.swapper then 'in'
            when new_tx_to = b.swapper then 'out'
            else null 
        end as direction,
        t.amount,
        t.mint,
        iff(_inner_index IS NULL, NULL, get(inner_instruction_program_ids, _inner_index-1)::string) as program_id
    from 
        {{ ref('silver__transfers') }} t 
        join base b 
            on b.block_timestamp::date = t.block_timestamp::date 
            and b.tx_id = t.tx_id 
            and b.swap_index = split_part(t.index,'.',1)::int
        left outer join solana.silver.token_account_owners ao_from
            on ao_from.account_address = t.tx_from
            and ao_from.start_block_id <= t.block_id
            and t.source_token_account = t.tx_from
        left outer join solana.silver.token_account_owners ao_to
            on ao_to.account_address = t.tx_to
            and ao_to.start_block_id <= t.block_id
            and t.dest_token_account = t.tx_to
    where 
        t.block_timestamp::date >= '2024-04-22'
        and new_tx_from <> new_tx_to
)
, swaps_inner as (
    select 
        t.tx_id,
        t._index as index,
        t._inner_index as inner_index,
        t.new_tx_from as tx_from,
        t.new_tx_to as tx_to,
        t.direction,
        t.amount,
        t.mint,
        t.program_id,
        t._index,
        t._inner_index,
        row_number() over (partition by t.tx_id, t._index, direction order by t._inner_index) as rn
    from transfers_modified_owners t
)
, lazy_matching as (
    select 
        i.tx_id, i.index, i.amount as from_amount, i.mint as from_mint, o.amount as to_amount, o.mint as to_mint, i.rn as swap_index, 
        case when i._inner_index < o._inner_index then 
            i.program_id
        else o.program_id 
        end as swap_program_id,
        'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4' as aggregator_program_id
    from swaps_inner i
    left outer join swaps_inner o
        on i.tx_id = o.tx_id
        and i._index = o._index
        and i.rn = o.rn 
        and i.direction <> o.direction
    where i.direction = 'in'
)
select *
from lazy_matching
order by swap_index
;