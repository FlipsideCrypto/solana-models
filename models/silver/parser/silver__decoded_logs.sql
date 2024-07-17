-- depends_on: {{ ref('silver__blocks') }}
-- depends_on: {{ ref('bronze__streamline_decoded_logs') }}
-- depends_on: {{ ref('bronze__streamline_FR_decoded_logs') }}
{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    unique_key = "decoded_logs_id",
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE','program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(tx_id, event_type, decoded_logs_id)'
    ),
    tags = ['scheduled_non_core']
) }}

{% set CUTOVER_DATETIME = modules.datetime.datetime.strptime("2024-07-16 17:00:00", "%Y-%m-%d %H:%M:%S") %}
{% set use_legacy_logic = False %}

/* run incremental timestamp value first then use it as a static value */
{% if execute %}
    {% if is_incremental() %}
        {% set max_inserted_query %}
            SELECT
                MAX(_inserted_timestamp) - INTERVAL '1 HOUR' AS _inserted_timestamp
            FROM
                {{ this }}
        {% endset %}

        {% set max_inserted_timestamp = run_query(max_inserted_query).columns[0].values()[0] %}
    {% endif %}

    {% if max_inserted_timestamp.replace(tzinfo=None) < CUTOVER_DATETIME %}
        {% set use_legacy_logic = True %}
        {% set create_tmp_query %}
            CREATE OR REPLACE TEMPORARY TABLE silver.decoded_logs__intermediate_tmp AS
            SELECT
                block_timestamp,
                block_id,
                tx_id,
                index,
                inner_index,
                log_index,
                program_id,
                data,
                _inserted_timestamp,
            FROM
                {% if is_incremental() %}
                {{ ref('bronze__streamline_decoded_logs') }} A
                {% else %}
                {{ ref('bronze__streamline_FR_decoded_logs') }} A
                {% endif %}
            JOIN
                {{ ref('silver__blocks') }}
                USING(block_id)
            {% if is_incremental() %}
            WHERE
                A._inserted_timestamp >= '{{ max_inserted_timestamp }}'
                AND A._partition_by_created_date_hour >= dateadd('hour', -2, date_trunc('hour','{{ max_inserted_timestamp }}'::timestamp_ntz))
            {% endif %}
        {% endset %}
        {% do run_query(create_tmp_query) %}

        {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.decoded_logs__intermediate_tmp","block_timestamp::date") %}
    {% endif %}

{% endif %}

{% if use_legacy_logic %}
WITH txs AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        signers,
        succeeded
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        {{ between_stmts }}
)
SELECT
    t.block_timestamp,
    d.block_id,
    d.tx_id,
    d.index,
    d.inner_index,
    d.log_index,
    t.signers,
    t.succeeded,
    d.program_id,
    d.data AS decoded_log,
    decoded_log :name :: STRING AS event_type,
    d._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['d.tx_id', 'd.index', 'd.inner_index','d.log_index']) }} AS decoded_logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    silver.decoded_logs__intermediate_tmp d
JOIN 
    txs t
    ON d.block_id = t.block_id
    AND d.tx_id = t.tx_id
QUALIFY
    ROW_NUMBER() over (
        PARTITION BY decoded_logs_id
        ORDER BY d._inserted_timestamp DESC
    ) = 1
{% else %}
SELECT
    value:request_data:block_timestamp::timestamp_ntz AS block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    log_index,
    value:request_data:signers::array AS signers,
    value:request_data:succeeded::boolean AS succeeded,
    program_id,
    data AS decoded_log,
    decoded_log:name::string AS event_type,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id', 'index', 'inner_index','log_index']) }} AS decoded_logs_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if is_incremental() %}
    {{ ref('bronze__streamline_decoded_logs') }}
    {% else %}
    {{ ref('bronze__streamline_FR_decoded_logs') }}
    {% endif %}
{% if is_incremental() %}
WHERE
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    AND _partition_by_created_date_hour >= dateadd('hour', -2, date_trunc('hour','{{ max_inserted_timestamp }}'::timestamp_ntz))
{% endif %}
QUALIFY
    row_number() OVER (PARTITION BY decoded_logs_id ORDER BY _inserted_timestamp DESC) = 1
{% endif %}
