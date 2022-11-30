{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id"],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

with base as (
    select 
        *
    from {{ ref('silver__swaps_intermediate_generic') }}
    {% if is_incremental() %}
    WHERE _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
    UNION 
    select 
        *
    from {{ ref('silver__swaps_intermediate_raydium') }}
    {% if is_incremental() %}
    WHERE _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),
base_swaps as (
    select 
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        program_id,
        swapper,
        from_mint,
        to_mint,
        _inserted_timestamp,
        min(swap_index) as swap_index,
        sum(from_amt) as from_amt,
        sum(to_amt) as to_amt
    from base 
    where from_amt is not null 
    and to_amt is not null
    group by 1,2,3,4,5,6,7,8,9
),
intermediate_swaps as (
    select 
        *,
        max(swap_index) over (partition by tx_id) as max_swap_index
    from base_swaps 
),
refunds as (
    select 
        tx_id,
        to_amt,
        to_mint
    from base
    where from_amt is null 
    and from_mint is null
    and to_amt is not null
),
fees as (
    select 
        tx_id,
        from_amt,
        from_mint
    from base
    where to_amt is null 
    and to_mint is null
    and from_amt is not null
)
, pre_final as (
    select
        b1.block_id,
        b1.block_timestamp,
        b1.tx_id,
        b1.succeeded,
        b1.swapper,
        b1.from_amt,
        b1.from_mint,
        coalesce(b2.to_amt,b1.to_amt) as to_amt,
        coalesce(b2.to_mint,b1.to_mint) as to_mint,
        b1._inserted_timestamp
    from intermediate_swaps b1
    left outer join intermediate_swaps b2 on b2.tx_id = b1.tx_id and b2.swap_index <> b1.swap_index and b2.swap_index > 1
    where b1.swap_index = 1
    and (b2.swap_index = b2.max_swap_index or b2.tx_id is null)
)
select 
    pf.block_id,
    pf.block_timestamp,
    pf.tx_id,
    pf.succeeded,
    pf.swapper,
    case when succeeded then
        pf.from_amt - coalesce(r.to_amt,0) + coalesce(f.from_amt,0) 
    else 0
    end as from_amt,
    pf.from_mint,
    case when succeeded then
        pf.to_amt - coalesce(f2.from_amt,0)
    else 0 
    end as to_amt,
    pf.to_mint,
    pf._inserted_timestamp
from pre_final pf
left outer join refunds r on r.tx_id = pf.tx_id and r.to_mint = pf.from_mint
left outer join fees f on f.tx_id = pf.tx_id and f.from_mint = pf.from_mint
left outer join fees f2 on f2.tx_id = pf.tx_id and f2.from_mint = pf.to_mint