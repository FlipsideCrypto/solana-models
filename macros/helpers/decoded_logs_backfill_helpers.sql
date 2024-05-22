{% macro decoded_logs_backfill_retries_generate_views(program_id, start_date, end_date, priority=None) %}
    {% set get_block_id_range_query %}
        select 
            min(block_id),
            max(block_id)
        from {{ ref('silver__blocks') }}
        where block_timestamp::date between '{{ start_date }}' and '{{ end_date }}'
    {% endset %}
    {% set range_results = run_query(get_block_id_range_query)[0] %}
    
    {% set min_block_id = range_results[0] %}
    {% set max_block_id = range_results[1] %}
    {% set step = 2000000 %}
    {% set retry_start_timestamp = modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") %}

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
                {{ '%011d' % start_block }}_{{ '%011d' % end_block }}_retry_{{ program_id }}
            {%- else -%}
                {{ '%02d' % priority }}_{{ '%011d' % start_block }}_retry_{{ '%011d' % end_block }}_{{ program_id }}
            {%- endif -%}
        {% endset %}

        {% set query %}
            create or replace view streamline.decoded_logs_backfill_{{ suffix }} AS 
            WITH completed_subset AS (
                SELECT
                    block_id,
                    program_id,
                    complete_decoded_instructions_2_id as id
                FROM
                    {{ ref('streamline__complete_decoded_instructions_2') }}
                WHERE
                    program_id = '{{ program_id }}'
                    AND block_id between {{ start_block }} and {{ end_block }}
                    AND _inserted_timestamp >= '{{ retry_start_timestamp }}'
            ),
            event_subset as (
                /*
                -- ONLY SUPPORTING JUPV6 ATM, only inner_instructiosn fro JUPV6 can be logs
                select 
                    e.block_id,
                    e.tx_id,
                    e.index,
                    NULL as inner_index,
                    e.instruction,
                    e.program_id,
                    e.block_timestamp,
                    {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','e.program_id']) }} as id
                from {{ ref('silver__events') }} e 
                where program_id = '{{ program_id }}'
                and block_id between {{ start_block }} and {{ end_block }}
                and succeeded
                union all
                */
                SELECT
                    e.block_id,
                    e.tx_id,
                    e.index,
                    i.index AS inner_index,
                    i.value AS instruction, 
                    i.value :programId :: STRING AS inner_program_id,
                    e.block_timestamp,
                    {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','inner_program_id']) }} AS id
                FROM 
                    {{ ref('silver__events') }} e ,
                    table(flatten(inner_instruction:instructions)) i
                WHERE 
                    array_contains('{{ program_id }}'::variant, inner_instruction_program_ids)
                    AND inner_program_id = '{{ program_id }}'
                    AND e.block_id BETWEEN {{ start_block }} AND {{ end_block }}
                    AND e.succeeded
                    AND array_size(i.value:accounts::array) = 1
            )
            SELECT 
                e.block_id,
                e.tx_id,
                e.index,
                e.inner_index,
                e.instruction,
                e.inner_program_id AS program_id,
                e.block_timestamp,
                e.id
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
    {% set sql_limit = 20000000 %}
    {% set producer_batch_size = 5000000 %}
    {% set worker_batch_size = 500000 %}
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
        func = schema_names[0] ~ ".udf_bulk_instructions_decoder(object_construct('sql_source', '" ~ table_name ~ "', 'external_table', 'decoded_instructions_2', 'sql_limit', '" ~ sql_limit ~ "', 'producer_batch_size', '" ~ producer_batch_size ~ "', 'worker_batch_size', '" ~ worker_batch_size ~ "', 'batch_call_limit', '" ~ batch_call_limit ~ "', 'call_type', 'backfill_logs'))",
        target = schema_names[0] ~ "." ~ table_name) %}
        
        {% do run_query(udf_call) %}
    {% endfor %}
{% endmacro %}