{{ config (
    materialized = "incremental",
    unique_key = "block_id",
    cluster_by = "_inserted_date",
    merge_update_columns = ["_inserted_date","_inserted_timestamp"],
    tags = ['streamline'],
) }}

WITH meta AS (

    SELECT
        registered_on,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "blocks_api") }}'
            )
        ) A
{% if is_incremental() %}
WHERE
    registered_on >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
{% endif %}
)
SELECT
    block_id,
    error,
    _inserted_date,
    m.registered_on as _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        "blocks_api"
    ) }} AS s
    JOIN meta m
    ON m.file_name = metadata$filename
WHERE
    s.block_id IS NOT NULL

{% if is_incremental() %}
    AND s._inserted_date >= CURRENT_DATE
    AND m.registered_on > (
        SELECT
            max(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
qualify(ROW_NUMBER() over (PARTITION BY block_id
ORDER BY
_inserted_date, _inserted_timestamp DESC)) = 1
