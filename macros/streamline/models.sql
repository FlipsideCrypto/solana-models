{% macro decode_logs_history(
        start,
        stop
    ) %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_max_block_by_date") }}
            qualify ROW_NUMBER() over (
                ORDER BY
                    block_number DESC
            ) = 1
    )
SELECT
    l.block_number,
    l._log_id,
    A.abi AS abi,
    OBJECT_CONSTRUCT(
        'topics',
        l.topics,
        'data',
        l.data,
        'address',
        l.contract_address
    ) AS DATA
FROM
    {{ ref("silver__logs") }}
    l
    INNER JOIN {{ ref("silver__complete_event_abis") }} A
    ON A.parent_contract_address = l.contract_address
    AND A.event_signature = l.topics[0]:: STRING
    AND l.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    (
        l.block_number BETWEEN {{ start }}
        AND {{ stop }}
    )
    AND l.block_number <= (
        SELECT
            block_number
        FROM
            look_back
    )
    AND _log_id NOT IN (
        SELECT
            _log_id
        FROM
            {{ ref("streamline__complete_decode_logs") }}
        WHERE
            (
                block_number BETWEEN {{ start }}
                AND {{ stop }}
            )
            AND block_number <= (
                SELECT
                    block_number
                FROM
                    look_back
            )
    )
{% endmacro %}

{% macro streamline_external_table_query(
        model,
        partition_function,
        partition_name,
        unique_key
    ) %}
    WITH meta AS (
        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS {{ partition_name }}
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", model) }}')
                ) A
            )
        SELECT
            {{ unique_key }},
            DATA,
            _inserted_timestamp,
            MD5(
                CAST(
                    COALESCE(CAST({{ unique_key }} AS text), '' :: STRING) AS text
                )
            ) AS id,
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
            AND (
                DATA :error :code IS NULL
                OR DATA :error :code NOT IN (
                    '-32000',
                    '-32001',
                    '-32002',
                    '-32003',
                    '-32004',
                    '-32005',
                    '-32006',
                    '-32007',
                    '-32008',
                    '-32009',
                    '-32010'
                )
            )
{% endmacro %}

{% macro streamline_external_table_FR_query(
        model,
        partition_function,
        partition_name,
        unique_key
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
    DATA,
    _inserted_timestamp,
    MD5(
        CAST(
            COALESCE(CAST({{ unique_key }} AS text), '' :: STRING) AS text
        )
    ) AS id,
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
    AND (
        DATA :error :code IS NULL
        OR DATA :error :code NOT IN (
            '-32000',
            '-32001',
            '-32002',
            '-32003',
            '-32004',
            '-32005',
            '-32006',
            '-32007',
            '-32008',
            '-32009',
            '-32010'
        )
    )
{% endmacro %}
