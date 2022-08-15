{{ config (
    materialized = "incremental",
    unique_key = "block_id",
    cluster_by = "ROUND(block_id, -5)",
    merge_update_columns = ["block_id"]
) }}

WITH meta AS (

    SELECT
        registered_on,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "solana_external", "blocks_api") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    registered_on >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    ),
    max_date AS (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
        {% else %}
    )
{% endif %}
SELECT
    block_id,
    m.registered_on AS _inserted_timestamp
FROM
    {{ source(
        "solana_external",
        "blocks_api"
    ) }} AS s
    JOIN meta m
    ON m.file_name = metadata$filename
WHERE
    s._inserted_date >= CURRENT_DATE -1
    AND s.block_id IS NOT NULL

{% if is_incremental() %}
AND m.registered_on > (
    SELECT
        max_INSERTED_TIMESTAMP
    FROM
        max_date
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_id
ORDER BY
    _inserted_timestamp DESC)) = 1
