-- depends_on: {{ ref('bronze__streamline_program_parser') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_id, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)",
    tags = ['streamline'],
) }}

SELECT
    block_id,
    concat_ws(
        '-',
        block_id,
        tx_id,
        program_id,
        index
    ) AS id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_program_parser') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp),'2000-01-01'::timestamp_ntz) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_program_parser') }}
{% endif %}
qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1 