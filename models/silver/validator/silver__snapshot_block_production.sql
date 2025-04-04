{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', epoch, node_pubkey)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  full_refresh = false,
  enabled = false,
  tags = ['deprecated']
) }}

select
    epoch :: INT as epoch,
    key as node_pubkey,
    f.value[0] :: INT as num_leader_slots,
    f.value[1] :: INT as num_blocks_produced,
    json_data:data:result:value:range:firstSlot :: INT as start_slot,
    json_data:data:result:value:range:lastSlot :: INT as end_slot,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['epoch', 'node_pubkey']
    ) }} AS snapshot_block_production_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from {{ source(
            'bronze_api',
            'block_production'
        ) }} a,
lateral flatten( input => json_data:data:result:value:byIdentity) as f
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
