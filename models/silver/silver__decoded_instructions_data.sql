{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, event_index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['program_id'],
) }}

SELECT
    tx_id,
    event_index,
    program_id,
    instruction_type,
    DATA,
    TO_TIMESTAMP_NTZ(
        SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER,
        0
    ) AS _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'decoded_instructions_data_api'
    ) }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id, event_index
ORDER BY
    _inserted_timestamp DESC)) = 1
