{{
    config(
        materialized="incremental",
        tags=['units_consumed_backfill']
    )
}}

{% if execute %}
    {% set max_partition = 120880 %}

    {% set next_partition_query %}
    {% if is_incremental() %}
        SELECT 
            LEAST(max(_partition_id) + 1, {{ max_partition }}), 
            LEAST(max(_partition_id) + 5, {{ max_partition }}) 
        FROM 
            {{ this }}
    {% else %}
        SELECT 24239, 24239 /* When computeUnitsConsumed first appears in node response */
    {% endif %}
    {% endset %}

    {% set next_partition = run_query(next_partition_query)[0][0] %}
    {% set next_partition_2 = run_query(next_partition_query)[0][1] %}

    -- list of partition IDs with >2m records to exclude
    {% set excluded_partitions = [
        116876, 116875, 116874, 116872, 116871, 116870, 116869, 116868, 116867, 
        116863, 116862, 116850, 105987, 103411, 103410, 103409, 103406, 51215, 
        45995, 31483, 31482, 27335, 27333, 27331, 27330, 27327, 27325
    ] %}
{% endif %}

SELECT 
    t.tx_id,
    t.data :meta :computeUnitsConsumed :: NUMBER AS units_consumed,
    t._partition_id,
    t._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id']) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    {{ ref('bronze__transactions2') }} t
WHERE 
    tx_id IS NOT NULL
    AND (
        COALESCE(t.data :transaction :message :instructions [0] :programId :: STRING, '') <> 'Vote111111111111111111111111111111111111111'
        OR (array_size(t.data :transaction :message :instructions) > 1)
    )
    AND _partition_id >= {{ next_partition }}
    AND _partition_id <= {{ next_partition_2 }}
    AND _partition_id NOT IN ({{ excluded_partitions | join(', ') }})
    qualify
        row_number() over(PARTITION BY tx_id ORDER BY _inserted_timestamp DESC) = 1