-- depends_on: {{ ref('bronze__streamline_blocks_2') }}
-- depends_on: {{ ref('bronze__streamline_FR_blocks_2') }}

{{ config (
    materialized = "incremental",
    unique_key = 'block_id',
    cluster_by = "ROUND(block_id, -6)",
) }}

SELECT
    block_id,
    error,
    _partition_by_created_date,
    _inserted_timestamp
FROM
    {% if is_incremental() %}
    {{ ref('bronze__streamline_blocks_2') }}
    {% else %}
    {{ ref('bronze__streamline_FR_blocks_2') }}
    {% endif %}
WHERE
    data IS NOT NULL
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            coalesce(max(_inserted_timestamp), '1970-01-01'::DATE) max_inserted_timestamp
        FROM
            {{ this }}
    )
    {% endif %}
QUALIFY
    row_number() OVER (
        PARTITION BY block_id
        ORDER BY _inserted_timestamp DESC
    ) = 1
