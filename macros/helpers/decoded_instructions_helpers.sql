{% macro generate_decoded_instructions_backfill_views(program_id) %}
    {% set result_cols = run_query("""select 
            first_block_id, 
            default_backfill_start_block_id
        from solana.streamline.idls_history
        where program_id = '""" ~ program_id ~ """';""").columns %}
    {% set min_block_id = result_cols[0].values()[0] | int %}
    {% set max_block_id = result_cols[1].values()[0] | int %}
    {% set step = 1000000 %}

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

        {% set query = """create or replace view streamline.decoded_instructions_backfill_""" ~ start_block ~ """_""" ~ end_block ~ """_""" ~ program_id ~ """ AS 
        with completed_subset AS (
            SELECT
                block_id,
                program_id,
                complete_decoded_instructions_2_id as id
            FROM
                """ ~ ref('streamline__complete_decoded_instructions_2') ~ """
            WHERE
                program_id = '""" ~ program_id ~ """'
            AND
                block_id between """ ~ start_block ~ """ and """ ~ end_block ~ """
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
                """ ~ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','e.program_id']) ~ """ as id
            from solana.silver.events e 
            where program_id = '""" ~ program_id ~ """'
            and block_id between """ ~ start_block ~ """ and """ ~ end_block ~ """
            and succeeded
            union 
            select
                e.block_id,
                e.tx_id,
                e.index,
                i.index as inner_index,
                i.value as instruction, 
                i.value :programId :: STRING AS inner_program_id,
                e.block_timestamp,
                """ ~ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','inner_program_id']) ~ """ as id
            from solana.silver.events e,
            table(flatten(inner_instruction:instructions)) i
            where array_contains(program_id::variant, inner_instruction_program_ids)
            and inner_program_id = '""" ~ program_id ~ """'
            and e.block_id between """ ~ start_block ~ """ and """ ~ end_block ~ """
            and e.succeeded
        )
        select 
            e.*
        from event_subset e
        left outer join completed_subset c on c.program_id = e.program_id and c.block_id = e.block_id and c.id = e.id
        where c.block_id is null""" %}

        {% do run_query(query) %}
    {% endfor %}

{% endmacro %}

{% macro queue_decoded_instructions_backfill() %}
    {% set sql_limit = 2500000 %}
    {% set producer_batch_size = 1000000 %}
    {% set worker_batch_size = 50000 %}
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
        from """ ~ ref('streamline__complete_decoded_instructions_2_backfill') ~ """
        order by 2 desc
        limit 10;""").columns %}
    {% set schema_names = results[0].values() %}
    {% set table_names = results[1].values() %}
    {% for table_name in table_names %}
        {% set udf_call = if_data_call_function(
        func = schema_names[0] ~ ".udf_bulk_instructions_decoder(object_construct('sql_source', '" ~ table_name ~ "', 'external_table', 'decoded_instructions_2', 'sql_limit', '" ~ sql_limit ~ "', 'producer_batch_size', '" ~ producer_batch_size ~ "', 'worker_batch_size', '" ~ worker_batch_size ~ "', 'batch_call_limit', '" ~ batch_call_limit ~ "', 'call_type', 'backfill'))",
        target = schema_names[0] ~ "." ~ table_name) %}
        
        {% do run_query(udf_call) %}
    {% endfor %}
{% endmacro %}

{% macro cleanup_decoded_instructions_backill_views() %}
    {% set results = run_query("""select
            table_schema,
            table_name
        from information_schema.views
        where table_name like 'DECODED_INSTRUCTIONS_BACKFILL_%'
        order by 2 desc
        limit 20;""").columns %}
    
    {% set schema_names = results[0].values() %}
    {% set table_names = results[1].values() %}
    {% for table_name in table_names %}
        {% set has_requests = run_query("""select 1 from """ ~ schema_names[0] ~ """.""" ~ table_name ~ """ limit 1""").columns[0].values()[0] %}
        {% if not has_requests %}
            {% do run_query("""drop view """ ~ schema_names[0] ~ """.""" ~ table_name) %}
            {% do run_query("""insert into """ ~ ref('streamline__complete_decoded_instructions_2_backfill') ~ """ values('""" ~ schema_names[0] ~ """','""" ~ table_name ~ """')""") %}
        {% endif %}
    {% endfor %}
{% endmacro %}