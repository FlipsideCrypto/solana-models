{% macro decoded_logs_backfill_generate_views(program_id, start_date, end_date, priority=None) %}
    {% set get_block_id_range_query %}
        SELECT 
            min(block_id),
            max(block_id)
        FROM
            {{ ref('silver__blocks') }}
        WHERE
            block_timestamp::date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    {% endset %}
    {% set range_results = run_query(get_block_id_range_query)[0] %}
    
    {% set min_block_id = range_results[0] %}
    {% set max_block_id = range_results[1] %}
    {% set step = 2000000 %}

    {% for i in range(min_block_id, max_block_id, step) %}
        {% if i == min_block_id %}
            {% set start_block = i %}
        {% else %}
            {% set start_block = i+1 %}
        {% endif %}

        {% if i+step >= max_block_id %}
            {% set end_block = max_block_id %}
        {% else %}
            {% set end_block = i+step %}
        {% endif %}

        {% set suffix %}
            {%- if priority is none -%}
                {{ '%011d' % start_block }}_{{ '%011d' % end_block }}_{{ program_id }}
            {%- else -%}
                {{ '%02d' % priority }}_{{ '%011d' % start_block }}_{{ '%011d' % end_block }}_{{ program_id }}
            {%- endif -%}
        {% endset %}

        {% set query %}
            CREATE OR REPLACE VIEW streamline.decoded_logs_backfill_{{ suffix }} AS 
            WITH completed_subset AS (
                SELECT
                    block_id,
                    program_id,
                    complete_decoded_logs_id as id
                FROM
                    {{ ref('streamline__complete_decoded_logs') }}
                WHERE
                    program_id = '{{ program_id }}'
                AND
                    block_id BETWEEN {{ start_block }} AND {{ end_block }}
            ),
            event_subset AS (
                SELECT
                    i.value :programId :: STRING AS inner_program_id,
                    e.tx_id,
                    e.index,
                    i.index AS inner_index,
                    NULL AS log_index,
                    i.value AS instruction,
                    e.block_id,
                    e.block_timestamp,
                    e.signers,
                    e.succeeded,
                    {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','log_index','inner_program_id']) }} as id
                FROM
                    {{ ref('silver__events') }} e
                JOIN
                    table(flatten(e.inner_instruction:instructions)) i 
                WHERE
                    e.block_id between {{ start_block }} and {{ end_block }}
                    AND e.succeeded
                    AND array_contains('{{ program_id }}'::variant, e.inner_instruction_program_ids)
                    AND inner_program_id = '{{ program_id }}'
                    AND array_size(i.value:accounts::array) = 1
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
                    l.block_id between {{ start_block }} and {{ end_block }}
                    AND l.program_id = '{{ program_id }}'
                    AND l.succeeded
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
                e.succeeded,
            FROM
                event_subset e
            LEFT OUTER JOIN
                completed_subset c
                ON c.program_id = e.inner_program_id
                AND c.block_id = e.block_id
                AND c.id = e.id
            WHERE
                c.block_id IS NULL
        {% endset %}

        {% do run_query(query) %}
    {% endfor %}
{% endmacro %}

{% macro decoded_logs_backill_cleanup_views() %}
    {% set results = run_query("""select
            table_schema,
            table_name
        from information_schema.views
        where table_name like 'DECODED_LOGS_BACKFILL_%'
        order by 2 desc
        limit 1;""").columns %}
    
    {% set schema_names = results[0].values() %}
    {% set table_names = results[1].values() %}
    {% for table_name in table_names %}
        {% set has_requests = run_query("""select 1 from """ ~ schema_names[0] ~ """.""" ~ table_name ~ """ limit 1""").columns[0].values()[0] %}
        {% if not has_requests %}
            {% do run_query("""drop view """ ~ schema_names[0] ~ """.""" ~ table_name) %}
            {% do run_query("""insert into """ ~ ref('streamline__complete_decoded_logs_backfill') ~ """ values('""" ~ schema_names[0] ~ """','""" ~ table_name ~ """')""") %}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro decoded_logs_backfill_calls() %}
    {% set sql_limit = 18000000 %} /* TODO switch this back to 20000000 after phoenix logs backfill completed */
    {% set producer_batch_size = 6000000 %} /* TODO switch this back to 5000000 after phoenix logs backfill completed */
    {% set worker_batch_size = 150000 %} /* TODO switch this back to 500000 after phoenix logs backfill completed */
    {% set batch_call_limit = 1000 %}

    {% set results = run_query("""select
            table_schema,
            table_name
        from information_schema.views
        where table_name like 'DECODED_LOGS_BACKFILL_%'
        except 
        select 
            schema_name,
            table_name
        from """ ~ ref('streamline__complete_decoded_logs_backfill') ~ """
        order by 2 desc
        limit 1;""").columns %}
    {% set schema_names = results[0].values() %}
    {% set table_names = results[1].values() %}
    {% for table_name in table_names %}
        {% set udf_call = if_data_call_function(
        func = schema_names[0] ~ ".udf_bulk_instructions_decoder(object_construct('sql_source', '" ~ table_name ~ "', 'external_table', 'decoded_logs', 'sql_limit', '" ~ sql_limit ~ "', 'producer_batch_size', '" ~ producer_batch_size ~ "', 'worker_batch_size', '" ~ worker_batch_size ~ "', 'batch_call_limit', '" ~ batch_call_limit ~ "', 'call_type', 'backfill_logs'))",
        target = schema_names[0] ~ "." ~ table_name) %}
        
        {% do run_query(udf_call) %}
    {% endfor %}
{% endmacro %}