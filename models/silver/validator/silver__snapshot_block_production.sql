{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', epoch, node_pubkey)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
) }}

select
    epoch :: INT as epoch,
    key as node_pubkey,
    f.value[0] :: INT as num_leader_slots,
    f.value[1] :: INT as num_blocks_produced,
    json_data:data[0]:result:value:range:firstSlot :: INT as start_slot,
    json_data:data[0]:result:value:range:lastSlot :: INT as end_slot,
    _inserted_timestamp
from {{ source(
            'bronze_api',
            'block_production'
        ) }} a,
lateral flatten( input => json_data:data[0]:result:value:byIdentity) as f
{% if is_incremental() %}
WHERE _inserted_timestamp > (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
qualify(ROW_NUMBER() over(PARTITION BY epoch, node_pubkey
ORDER BY
    _inserted_timestamp DESC)) = 1

