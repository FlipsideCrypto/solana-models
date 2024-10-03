-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = "swaps_intermediate_jupiterv4_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(tx_id, swapper, from_mint, to_mint)'
    ),
    tags = ['scheduled_non_core'],
) }}

{% if execute %}
    {% set base_query %}
    CREATE
    OR REPLACE temporary TABLE silver.swaps_intermediate_jupiterv4__intermediate_tmp AS

    SELECT
        block_timestamp,
        block_id,
        succeeded,
        tx_id,
        index,
        inner_index,
        program_id,
        decoded_instruction,
        event_type,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'
        AND event_type in ('route','raydiumSwapExactOutput','raydiumClmmSwapExactOutput','whirlpoolSwapExactOutput')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE > '2023-01-20'
{% endif %}

{% endset %}
{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.swaps_intermediate_jupiterv4__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}



WITH base AS (
    SELECT
        *
    FROM
        silver.swaps_intermediate_jupiterv4__intermediate_tmp
),

swaps as (
select 
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        log_index,
        swap_index,
        swapper,
        from_mint,
        from_amount,
        to_mint,
        to_amount,
        _inserted_timestamp
from {{ ref('silver__swaps_inner_intermediate_jupiterv4') }}
    WHERE
        {{ between_stmts }}
)
,
dest_swap as (
select   a.block_timestamp,
a.block_id,
a.tx_id,
a.succeeded,
a.program_id,
a.index,
a.inner_index,
b.log_index,
b.swap_index,
b.swapper,
b.to_mint,
b.to_amount,
a._inserted_timestamp
    from base a
    left join swaps b
    on a.block_timestamp::date = b.block_timestamp::date 
        and a.tx_id = b.tx_id
        and a.index = b.index
        AND coalesce(a.inner_index,-1) = coalesce(b.inner_index,-1)
    QUALIFY
            row_number() OVER (PARTITION BY b.tx_id, b.index,b.inner_index ORDER BY coalesce(b.swap_index,-1) desc) = 1

)
-- select * from dest_swap;
,
source_swap as (
select     a.block_timestamp,
a.block_id,
a.tx_id,
a.succeeded,
a.program_id,
a.index,
a.inner_index,
b.log_index,
b.swap_index,
b.swapper,
b.from_mint,
b.from_amount,
a._inserted_timestamp
    from base a
    left join swaps b
    on a.block_timestamp::date = b.block_timestamp::date 
        and a.tx_id = b.tx_id
        and a.index = b.index
        AND coalesce(a.inner_index,-1) = coalesce(b.inner_index,-1)
    where swap_index = 0

)

select 
a.block_timestamp,
a.block_id,
a.tx_id,
a.index,
a.inner_index,
row_number() OVER (PARTITION BY a.tx_id ORDER BY a.index, a.inner_index)-1 AS swap_index,
a.succeeded,
a.program_id,
a.swapper,
a.from_mint,
a.from_amount,
b.to_mint,
b.to_amount,
    a._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id','a.index','a.inner_index']) }} AS swaps_intermediate_jupiterv4_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from source_swap a
left join dest_swap b
on a.tx_id = b.tx_id
and a.index = b.index