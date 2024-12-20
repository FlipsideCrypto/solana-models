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
        unique_key,
        other_cols
    ) %}
    WITH meta AS (
        SELECT
            LAST_MODIFIED AS _inserted_timestamp,
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
            {% if unique_key is not none and unique_key != "" %}
            {{ unique_key }},
            {% endif %}
            {% if other_cols is not none and other_cols != "" %}
            {{ other_cols }},
            {% endif %}
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
            LAST_MODIFIED AS _inserted_timestamp,
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
    {% if unique_key is not none and unique_key != "" %}
    {{ unique_key }},
    {% endif %}
    {% if other_cols is not none and other_cols != "" %}
    {{ other_cols }},
    {% endif %}
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


{% macro streamline_external_table_query_v2(
        model,
        partition_function,
        partition_name,
        unique_key,
        other_cols
) %}
WITH meta AS (
    SELECT
        LAST_MODIFIED AS _inserted_timestamp,
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
    {% if other_cols is not none and other_cols != "" %}
    {{ other_cols }},
    {% endif %}
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
JOIN 
    meta b
    ON b.file_name = metadata$filename
    AND b.{{ partition_name }} = s.{{ partition_name }}
WHERE
    b.{{ partition_name }} = s.{{ partition_name }}
    AND (
        data:error:code IS NULL
        OR data:error:message::string LIKE '%Slot % was skipped, or missing in long-term storage%'
        OR data:error:message::string LIKE 'Slot % was skipped, or missing due to ledger jump to recent snapshot'
    )
{% endmacro %}

{% macro streamline_external_table_FR_query_v2(
        model,
        partition_function,
        partition_name,
        unique_key,
        other_cols
) %}
WITH meta AS (
    SELECT
        LAST_MODIFIED AS _inserted_timestamp,
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
    {% if other_cols is not none and other_cols != "" %}
    {{ other_cols }},
    {% endif %}
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
JOIN 
    meta b
    ON b.file_name = metadata$filename
    AND b.{{ partition_name }} = s.{{ partition_name }}
WHERE
    b.{{ partition_name }} = s.{{ partition_name }}
    AND (
        data:error:code IS NULL
        OR data:error:message::string LIKE '%Slot % was skipped, or missing in long-term storage%'
        OR data:error:message::string LIKE 'Slot % was skipped, or missing due to ledger jump to recent snapshot'
    )
{% endmacro %}