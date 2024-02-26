select b.block_id
from solana.silver.blocks b
left outer join {{ ref('silver___blocks_tx_count') }} b2 on b.block_id = b2.block_id
where b.block_id >= 226000000
and b.block_timestamp::date between current_date - 8 and current_date - 1
and b2.block_id is null