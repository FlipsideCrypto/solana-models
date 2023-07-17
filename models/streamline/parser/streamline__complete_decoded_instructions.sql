-- depends_on: {{ ref('bronze__streamline_program_parser') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_id, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

SELECT
    block_id,
    CONCAT_WS('-', block_id, data[1]:program::STRING, data[0]) AS id,
    _inserted_timestamp
FROM
{% if is_incremental() %}
{{ ref('bronze__streamline_program_parser') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_program_parser') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
