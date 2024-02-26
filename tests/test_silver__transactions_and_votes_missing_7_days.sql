

with solscan_counts as (
    select s.*
    from solana.silver._blocks_tx_count s
    join solana.silver.blocks b on b.block_id = s.block_id 
    where b.block_timestamp::date between current_date - 8 and current_date - 1
),
silver_counts as (
    select block_id, sum(transaction_count) as transaction_count
    from (
        select block_id, count(block_id) as transaction_count 
        from {{ ref('silver__transactions') }} t 
        where block_timestamp::date between current_date - 8 and current_date - 1
        group by 1
        union all 
        select block_id, count(block_id) as transaction_count 
        from solana.silver.votes t 
        where block_timestamp::date between current_date - 8 and current_date - 1
        group by 1
    )
    group by 1
)
select 
    e.block_id,
    e.transaction_count as ect,
    a.transaction_count as act,
    e.transaction_count - a.transaction_count as delta 
from solscan_counts e 
left outer join silver_counts a 
    on e.block_id = a.block_id
where 
    delta <> 0
    or a.block_id is null