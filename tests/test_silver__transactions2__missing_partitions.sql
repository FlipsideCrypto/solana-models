with max_part_id_tmp as (
    select max(_partition_id) as _partition_id
    from solana.silver.votes2
    union 
    select max(_partition_id)
    from solana.silver.transactions2
),
base as (
  select distinct _partition_id
  from solana.streamline.complete_block_txs
  where _partition_id <= (select max(_partition_id) from max_part_id_tmp)
),
base_txs as (
    select distinct _partition_id
    from {{ ref('silver__transactions2') }}
    union 
    select distinct _partition_id
    from solana.silver.votes2
)
select b._partition_id 
from base b
left outer join base_txs t on b._partition_id = t._partition_id 
where t._partition_id is null
and b._partition_id <> 1877 -- seems like this whole partition is skipped slots
