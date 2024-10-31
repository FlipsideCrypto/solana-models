{{
    config(
        materialized="incremental",
        unique_key=['tx_id'],
        tags=['units_consumed_backfill']
    )
}}

{% if execute %}
    {% set next_partition_query %}
    {% if is_incremental() %}
        SELECT max(_partition_id)+1, max(_partition_id)+5 FROM {{ this }}
    {% else %}
        SELECT 24239, 24239 /* When computeUnitsConsumed first appears in node response */
    {% endif %}
    {% endset %}

    {% set next_partition = run_query(next_partition_query)[0][0] %}
    {% set next_partition_2 = run_query(next_partition_query)[0][1] %}
{% endif %}

WITH pre_final AS (
    SELECT 
        t.tx_id,
        t.data :meta :computeUnitsConsumed :: NUMBER as compute_units_consumed,
        t._partition_id,
        t._inserted_timestamp
    FROM 
        {{ ref('bronze__transactions2') }} t
    WHERE 
        tx_id IS NOT NULL
        AND (
            COALESCE(t.data :transaction :message :instructions [0] :programId :: STRING,'') <> 'Vote111111111111111111111111111111111111111'
            OR
            (
                array_size(t.data :transaction :message :instructions) > 1
            )
        )
        AND _partition_id >= {{ next_partition }}
        AND _partition_id <= {{ next_partition_2 }}
)

SELECT
    tx_id,
    compute_units_consumed as units_consumed,
    _partition_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id']) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final 