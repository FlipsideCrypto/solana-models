{% macro decoded_instructions_backfill_generate_views(program_id, priority=None) %}
    {% set result_cols = run_query("""select 
            first_block_id, 
            default_backfill_start_block_id
        from """ ~ ref('streamline__idls_history') ~ """ 
        where program_id = '""" ~ program_id ~ """';""").columns %}
    {% set min_block_id = result_cols[0].values()[0] | int %}
    {% set max_block_id = result_cols[1].values()[0] | int %}
    {% set step = 10000000 %}

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
            create or replace view streamline.decoded_instructions_backfill_{{ suffix }} AS 
            with completed_subset AS (
                SELECT
                    block_id,
                    program_id,
                    complete_decoded_instructions_3_id as id
                FROM
                    {{ ref('streamline__complete_decoded_instructions_3') }}
                WHERE
                    program_id = '{{ program_id }}'
                AND
                    block_id between {{ start_block }} and {{ end_block }}
            ),
            event_subset as (
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
                select
                    e.block_id,
                    e.tx_id,
                    e.index,
                    i.index as inner_index,
                    i.value as instruction, 
                    i.value :programId :: STRING AS inner_program_id,
                    e.block_timestamp,
                    {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','inner_program_id']) }} as id
                from {{ ref('silver__events') }} e ,
                table(flatten(inner_instruction:instructions)) i
                where array_contains('{{ program_id }}'::variant, inner_instruction_program_ids)
                and inner_program_id = '{{ program_id }}'
                and e.block_id between {{ start_block }} and {{ end_block }}
                and e.succeeded
            )
            select 
                e.*
            from event_subset e
            left outer join completed_subset c on c.program_id = e.program_id and c.block_id = e.block_id and c.id = e.id
            where c.block_id is null
        {% endset %}

        {% do run_query(query) %}
    {% endfor %}
{% endmacro %}

-- USE THIS TO BACKFILL PARSER 2.0 ERRORS
-- THIS ONLY HANDLES ERRORS ie: `decoded_instruction:error::string is not null`
{% macro decoded_instructions_backfill_retries_generate_views(program_id, start_date, end_date, priority=None) %}
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
    {% set retry_start_timestamp = modules.datetime.datetime.now(modules.pytz.utc).strftime("%Y-%m-%d %H:%M:%S") %}

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
                {{ '%02d' % priority }}_{{ '%011d' % start_block }}_{{ '%011d' % end_block }}_retry_{{ program_id }}
            {%- endif -%}
        {% endset %}

        {% set query %}
            create or replace view streamline.decoded_instructions_backfill_{{ suffix }} AS
            with retries as (
                select block_id, tx_id, index, inner_index
                from {{ ref('silver__decoded_instructions_combined') }}
                where program_id = '{{ program_id }}'
                and succeeded
                and decoded_instruction:error::string is not null
                and block_id between {{ start_block }} and {{ end_block }}

            ),
            retry_events as (
                select 
                    e.block_id,
                    e.tx_id,
                    e.index,
                    NULL as inner_index,
                    e.instruction,
                    e.program_id,
                    e.block_timestamp,
                    {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','e.program_id']) }} as id
                from retries r
                join {{ ref('silver__events') }} e 
                    on r.block_id = e.block_id 
                    and r.tx_id = e.tx_id 
                    and r.index = e.index 
                where e.program_id = '{{ program_id }}'
                and e.block_id between {{ start_block }} and {{ end_block }}
                and e.succeeded
                and r.inner_index is null
                union all
                select
                    e.block_id,
                    e.tx_id,
                    e.index,
                    i.index as inner_index,
                    i.value as instruction, 
                    i.value :programId :: STRING AS inner_program_id,
                    e.block_timestamp,
                    {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','inner_program_id']) }} as id
                from retries r
                join {{ ref('silver__events') }} e 
                    on r.block_id = e.block_id 
                    and r.tx_id = e.tx_id 
                    and r.index = e.index 
                join table(flatten(inner_instruction:instructions)) i
                where array_contains('{{ program_id }}'::variant, inner_instruction_program_ids)
                and inner_program_id = '{{ program_id }}'
                and e.block_id between {{ start_block }} and {{ end_block }}
                and e.succeeded
                and r.inner_index is not null
                and r.inner_index = i.index
            ),
            completed_subset as (
                select 
                    *
                from 
                    {{ ref('streamline__complete_decoded_instructions_3') }}
                where 
                    program_id = '{{ program_id }}'
                and 
                    _inserted_timestamp >= '{{ retry_start_timestamp }}'
            )
            select 
                e.*
            from retry_events e 
            left outer join completed_subset c 
                    on c.complete_decoded_instructions_3_id = e.id
            where 
                c.complete_decoded_instructions_3_id is null
            ;
        {% endset %}

        {% do run_query(query) %}
    {% endfor %}
{% endmacro %}

{% macro decoded_instructions_backill_cleanup_views() %}
    {% set results = run_query("""select
            table_schema,
            table_name
        from information_schema.views
        where table_name like 'DECODED_INSTRUCTIONS_BACKFILL_%'
        order by 2 desc
        limit 1;""").columns %}
    
    {% set schema_names = results[0].values() %}
    {% set table_names = results[1].values() %}
    {% for table_name in table_names %}
        {% set has_requests = run_query("""select 1 from """ ~ schema_names[0] ~ """.""" ~ table_name ~ """ limit 1""").columns[0].values()[0] %}
        {% if not has_requests %}
            {% do run_query("""drop view """ ~ schema_names[0] ~ """.""" ~ table_name) %}
            {% do run_query("""insert into """ ~ ref('streamline__complete_decoded_instructions_3_backfill') ~ """ values('""" ~ schema_names[0] ~ """','""" ~ table_name ~ """')""") %}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro decoded_instructions_backfill_calls() %}
    {% set sql_limit = 20000000 %}
    {% set producer_batch_size = 5000000 %}
    {% set worker_batch_size = 500000 %}
    {% set batch_call_limit = 1000 %}

    {% set results = run_query("""select
            table_schema,
            table_name
        from information_schema.views
        where table_name like 'DECODED_INSTRUCTIONS_BACKFILL_%'
        except 
        select 
            schema_name,
            table_name
        from """ ~ ref('streamline__complete_decoded_instructions_3_backfill') ~ """
        order by 2 desc
        limit 1;""").columns %}
    {% set schema_names = results[0].values() %}
    {% set table_names = results[1].values() %}
    {% for table_name in table_names %}
        {% set udf_call = if_data_call_function(
        func = schema_names[0] ~ ".udf_bulk_instructions_decoder_v2(object_construct('sql_source', '" ~ table_name ~ "', 'external_table', 'decoded_instructions_3', 'sql_limit', '" ~ sql_limit ~ "', 'producer_batch_size', '" ~ producer_batch_size ~ "', 'worker_batch_size', '" ~ worker_batch_size ~ "', 'batch_call_limit', '" ~ batch_call_limit ~ "', 'call_type', 'backfill'))",
        target = schema_names[0] ~ "." ~ table_name) %}
        
        {% do run_query(udf_call) %}
    {% endfor %}
{% endmacro %}