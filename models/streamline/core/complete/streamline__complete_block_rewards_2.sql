{{ config (
    materialized = "incremental",
    unique_key = 'block_id',
    cluster_by = "ROUND(block_id, -6)",
) }}

SELECT
    block_id,
    error,
    _partition_id,
    _inserted_timestamp
FROM
{% if is_incremental() %}
    {{ ref('bronze__streamline_block_rewards') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_block_rewards') }}
{% endif %}
QUALIFY
    row_number() OVER (PARTITION BY block_id ORDER BY _inserted_timestamp DESC) = 1