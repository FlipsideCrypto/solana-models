{% macro streamline_external_table_query(
        model,
        partition_function,
        partition_name,
        unique_key,
        other_cols
    ) %}
    WITH meta AS (
        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS {{ partition_name }}
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -7, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", model) }}')
                ) A
            )
        SELECT
            {{ unique_key }},
            {{ other_cols }},
            DATA,
            _inserted_timestamp,
            s.{{ partition_name }},
            s.value AS VALUE
        FROM
            {{ source(
                "bronze_streamline",
                model
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.{{ partition_name }} = s.{{ partition_name }}
        WHERE
            b.{{ partition_name }} = s.{{ partition_name }}
            AND DATA :error :code IS NULL
{% endmacro %}

{% macro streamline_external_table_FR_query(
        model,
        partition_function,
        partition_name,
        unique_key,
        other_cols
    ) %}
    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS {{ partition_name }}
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", model) }}'
                )
            ) A
    )
SELECT
    {{ unique_key }},
     {{ other_cols }},
    DATA,
    _inserted_timestamp,
    s.{{ partition_name }},
    s.value AS VALUE
FROM
    {{ source(
        "bronze_streamline",
        model
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b.{{ partition_name }} = s.{{ partition_name }}
WHERE
    b.{{ partition_name }} = s.{{ partition_name }}
    AND DATA :error :code IS NULL
    
{% endmacro %}
