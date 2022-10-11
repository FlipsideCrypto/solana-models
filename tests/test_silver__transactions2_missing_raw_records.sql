with prev_day_max_block as (
  select max(block_id) as block_id
  from solana.silver.blocks2
  where block_timestamp::date = current_date - 1
  and block_id >= 154195836
),
base as (
  select block_id
  from solana.streamline.complete_block_txs
  where block_id >= 154195836
  and block_id <= (select block_id from prev_day_max_block limit 1)
  and error is null
),
base_txs as (
    select distinct block_id
    from {{ ref('silver__transactions2') }}
    where block_id >= 154195836
    and block_id <= (select block_id from prev_day_max_block limit 1)
    union 
    select distinct block_id
    from {{ ref('silver__votes2') }}
    where block_id >= 154195836
    and block_id <= (select block_id from prev_day_max_block limit 1)
)
select block_id 
from base b
left outer join base_txs t on b.block_id = t.block_id 
where t.block_id is null

