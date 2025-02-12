-- depends_on: {{ ref('silver__blocks') }}
{{ config (
    materialized = "table",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_instructions_decoder_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'decoded_logs_2', 'sql_limit', {{var('sql_limit','5000000')}}, 'producer_batch_size', {{var('producer_batch_size','2000000')}}, 'worker_batch_size', {{var('worker_batch_size','100000')}}, 'batch_call_limit', {{var('batch_call_limit','1000')}}, 'call_type', 'logs'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_decoder_logs']
) }}

{% if execute %}
    {% set min_event_block_id_query %}
        SELECT
            min(block_id)
        FROM 
            {{ ref('silver__blocks') }}
        WHERE 
            block_timestamp >= CURRENT_DATE - 2
    {% endset %}
    {% set min_event_block_id = run_query(min_event_block_id_query).columns[0].values()[0] %}
    {% set idls_to_decode = "'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4','PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY','6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P','DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M','7a4WjyR8VZ7yZz5XJAKm39BUGn5iT9CKcv2pmG9tdXVH'" %}
    {% set programs_from_tx_logs_to_decode = "'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN','JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'" %}
{% endif %}

WITH event_subset AS (
    SELECT
        e.program_id AS inner_program_id,
        e.tx_id,
        e.instruction_index AS index,
        e.inner_index,
        NULL AS log_index,
        e.instruction,
        e.block_id,
        e.block_timestamp,
        e.signers,
        e.succeeded,
        {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.instruction_index','inner_index','log_index','inner_program_id']) }} as id
    FROM
        {{ ref('silver__events_inner') }} AS e
    WHERE
        e.block_timestamp >= CURRENT_DATE - 2
        AND e.succeeded
        AND e.program_id IN ({{ idls_to_decode }})
        AND array_size(e.instruction:accounts::array) = 1
    UNION ALL
    SELECT 
        l.program_id,
        l.tx_id,
        l.index,
        l.inner_index,
        l.log_index,
        object_construct('accounts',[],'data',l.data,'programId',l.program_id) as instruction,
        l.block_id,
        l.block_timestamp,
        t.signers,
        t.succeeded,
        {{ dbt_utils.generate_surrogate_key(['l.block_id','l.tx_id','l.index','l.inner_index','l.log_index','l.program_id']) }} as id
    FROM
        {{ ref('silver__transaction_logs_program_data') }} l
    JOIN
        {{ ref('silver__transactions') }} t
        USING(block_timestamp, tx_id)
    WHERE 
        l.block_timestamp >= CURRENT_DATE - 2
        AND l.program_id IN ({{ programs_from_tx_logs_to_decode }})
),
completed_subset AS (
    SELECT
        block_id,
        complete_decoded_logs_2_id as id
    FROM
        {{ ref('streamline__complete_decoded_logs_2') }}
    WHERE
        block_id >= {{ min_event_block_id }} --ensure we at least prune to last 2 days worth of blocks since the dynamic below will scan everything
        AND block_id >= (
            SELECT
                MIN(block_id)
            FROM
                event_subset
        )
)
SELECT
    e.inner_program_id as program_id,
    e.tx_id,
    e.index,
    e.inner_index,
    e.log_index,
    e.instruction,
    e.block_id,
    e.block_timestamp,
    e.signers,
    e.succeeded
FROM
    event_subset AS e
LEFT OUTER JOIN 
    completed_subset AS c
    ON c.block_id = e.block_id
    AND e.id = c.id
WHERE
    c.block_id IS NULL