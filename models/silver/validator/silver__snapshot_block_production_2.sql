{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', epoch, node_pubkey, start_slot, end_slot)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  tags = ['validator']
) }}

{% set historical_epoch_cutoff = 552 %}

SELECT
    epoch::INT AS epoch,
    key AS node_pubkey,
    f.value[0]::INT AS num_leader_slots,
    f.value[1]::INT AS num_blocks_produced,
    json_data:data:result:value:range:firstSlot::INT AS start_slot,
    json_data:data:result:value:range:lastSlot::INT AS end_slot,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['epoch', 'node_pubkey', 'start_slot', 'end_slot']
    ) }} AS snapshot_block_production_2_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    {{ source('bronze_api', 'block_production') }} a,
    LATERAL FLATTEN(input => json_data:data:result:value:byIdentity) AS f
WHERE
    epoch > {{ historical_epoch_cutoff }}
    {% if is_incremental() %}
    AND _inserted_timestamp > (
        SELECT
            max(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
QUALIFY 
    row_number() OVER (
        PARTITION BY epoch, node_pubkey, start_slot, end_slot
        ORDER BY _inserted_timestamp DESC
    ) = 1

