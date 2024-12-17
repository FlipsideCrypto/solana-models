-- depends_on: {{ ref('bronze__streamline_block_txs_2') }}
-- depends_on: {{ ref('bronze__streamline_FR_block_txs_2') }}

{{ config (
    materialized = "incremental",
    unique_key = 'block_id',
    cluster_by = "ROUND(block_id, -6)",
) }}

SELECT
    block_id,
    _partition_id,
    max(_inserted_timestamp) AS _inserted_timestamp
FROM
{% if is_incremental() %}
    {{ ref('bronze__streamline_block_txs_2') }}
WHERE
    _partition_id > (SELECT coalesce(max(_partition_id), 0) FROM {{ this }})
    AND _inserted_timestamp >= (SELECT coalesce(max(_inserted_timestamp), '1970-01-01' :: DATE) FROM {{ this }})
{% else %}
    {{ ref('bronze__streamline_FR_block_txs_2') }}
{% endif %}
GROUP BY 1,2