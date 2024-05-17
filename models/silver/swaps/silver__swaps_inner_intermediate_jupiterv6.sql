-- depends_on: {{ ref('silver__swaps_intermediate_jupiterv6') }}

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
            _inserted_timestamp,
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
            AND
            _inserted_timestamp < (
                SELECT
                    MAX(_inserted_timestamp) + INTERVAL '10 hour'
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
        program_id,
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
        block_id,
        tx_id,
        split_part(index,'.',1)::int AS index,
        split_part(index,'.',2)::int AS inner_index,
        tx_from,
        tx_to,
        mint, 
        amount,
        source_token_account,
        dest_token_account
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
        e.inner_instruction_program_ids,
        j._inserted_timestamp
    from 
        base_swaps j
        join 
            base_decoded d 
            on d.block_timestamp::date = j.block_timestamp::date
            and d.tx_id = j.tx_id 
            and d.index = j.swap_index 
            and d.program_id = j.program_id
        join 
            base_events e
            on d.block_timestamp::date = e.block_timestamp::date
            and d.tx_id = e.tx_id
            and d.index = e.index
),
transfers_modified_owners AS (
    select 
        t.block_timestamp,
        t.block_id,
        t.tx_id,
        t.index,
        t.inner_index,
        b.swapper,
        silver.udf_get_account_pubkey_by_name('programAuthority', decoded_instruction:accounts) AS acting_authority,
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
        iff(t.inner_index IS NULL, NULL, get(inner_instruction_program_ids, t.inner_index-1)::string) as program_id,
        b._inserted_timestamp
    from 
        base_transfers t 
        join 
            base b 
            on b.block_timestamp::date = t.block_timestamp::date 
            and b.tx_id = t.tx_id 
            and b.swap_index = t.index
        left outer join 
            {{ ref('silver__token_account_owners') }} ao_from
            on ao_from.account_address = t.tx_from
            and ao_from.start_block_id <= t.block_id
            and t.source_token_account = t.tx_from
        left outer join 
            {{ ref('silver__token_account_owners') }} ao_to
            on ao_to.account_address = t.tx_to
            and ao_to.start_block_id <= t.block_id
            and t.dest_token_account = t.tx_to
    where 
        new_tx_from <> new_tx_to
),
swaps_inner_tmp as (
    select 
        t.block_timestamp,
        t.block_id,
        t.tx_id,
        t.index,
        t.inner_index,
        t.swapper,
        t.new_tx_from as tx_from,
        t.new_tx_to as tx_to,
        t.direction,
        t.amount,
        t.mint,
        t.program_id,
        row_number() over (partition by t.tx_id, t.index, direction order by t.inner_index) as rn,
        t._inserted_timestamp
    from transfers_modified_owners t
),
swaps_inner as (
    select 
        s1.block_timestamp,
        s1.block_id,
        s1.tx_id,
        s1.index,
        s1.inner_index,
        s1.swapper,
        s1.tx_from,
        s1.tx_to,
        s1.direction,
        s1.amount,
        coalesce(s1.mint, s2.mint) as mint,
        s1.program_id,
        s1.rn,
        s1._inserted_timestamp,
    from swaps_inner_tmp s1
        left outer join swaps_inner_tmp s2 
        on s1.tx_id = s2.tx_id 
        and s1.tx_to = s2.tx_from
        and s1.rn = s2.rn
),
lazy_matching as (
    select 
        i.block_timestamp,
        i.block_id,
        i.tx_id, 
        i.index, 
        i.inner_index,
        i.swapper,
        i.amount as from_amount, 
        i.mint as from_mint, 
        o.amount as to_amount, 
        o.mint as to_mint, 
        i.rn as swap_index, 
        max(i.rn) over (partition by i.tx_id, i.index) as max_swap_index,
        case 
            when i.inner_index < o.inner_index then 
                i.program_id
            else 
                o.program_id 
        end as swap_program_id,
        'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4' as aggregator_program_id,
        i._inserted_timestamp
    from swaps_inner i
        left outer join swaps_inner o
        on i.tx_id = o.tx_id
        and i.index = o.index
        and i.rn = o.rn 
        and i.direction <> o.direction
    where i.direction = 'in'
)
select 
    block_timestamp,
    block_id,
    tx_id, 
    index, 
    inner_index,
    swapper,
    from_amount, 
    from_mint, 
    to_amount, 
    to_mint, 
    swap_index, 
    swap_program_id,
    aggregator_program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index']) }} as swaps_inner_intermediate_jupiterv6_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
from lazy_matching
WHERE 
    (
        to_mint is not null 
        OR
        (to_mint is null and swap_index = 1 and swap_index = max_swap_index)
    )