{% macro apply_meta_as_tags(results) %}
    {% if var("UPDATE_SNOWFLAKE_TAGS") %}
        {{ log('apply_meta_as_tags', info=False) }}
        {{ log(results, info=False) }}
        {% if execute %}
            
            {%- set tags_by_schema = {} -%}
            {% for res in results -%}
                {% if res.node.meta.database_tags %}
                    
                    {%- set model_database = res.node.database -%}
                    {%- set model_schema = res.node.schema -%}
                    {%- set model_schema_full = model_database+'.'+model_schema -%}
                    {%- set model_alias = res.node.alias -%}

                    {% if model_schema_full not in tags_by_schema.keys() %}
                        {{ log('need to fetch tags for schema '+model_schema_full, info=False) }}
                        {%- call statement('main', fetch_result=True) -%}
                            show tags in {{model_database}}.{{model_schema}}
                        {%- endcall -%}
                        {%- set _ = tags_by_schema.update({model_schema_full: load_result('main')['table'].columns.get('name').values()|list}) -%}
                        {{ log('Added tags to cache', info=False) }}
                    {% else %}
                        {{ log('already have tag info for schema', info=False) }}
                    {% endif %}

                    {%- set current_tags_in_schema = tags_by_schema[model_schema_full] -%}
                    {{ log('current_tags_in_schema:', info=False) }}
                    {{ log(current_tags_in_schema, info=False) }}
                    {{ log("========== Processing tags for "+model_schema_full+"."+model_alias+" ==========", info=False) }}

                    {% set line -%}
                        node: {{ res.node.unique_id }}; status: {{ res.status }} (message: {{ res.message }})
                        node full: {{ res.node}}
                        meta: {{ res.node.meta}}
                        materialized: {{ res.node.config.materialized }}
                    {%- endset %}
                    {{ log(line, info=False) }}

                    {%- call statement('main', fetch_result=True) -%}
                        select LEVEL,UPPER(TAG_NAME) as TAG_NAME,TAG_VALUE from table(information_schema.tag_references_all_columns('{{model_schema}}.{{model_alias}}', 'table'))
                    {%- endcall -%}
                    {%- set existing_tags_for_table = load_result('main')['data'] -%}
                    {{ log('Existing tags for table:', info=False) }}
                    {{ log(existing_tags_for_table, info=False) }}

                    {{ log('--', info=False) }}
                    {% for table_tag in res.node.meta.database_tags.table %}

                        {{ create_tag_if_missing(current_tags_in_schema,table_tag|upper) }}
                        {% set desired_tag_value = res.node.meta.database_tags.table[table_tag] %}

                        {{set_table_tag_value_if_different(model_schema,model_alias,table_tag,desired_tag_value,existing_tags_for_table)}}
                    {% endfor %}
                    {{ log("========== Finished processing tags for "+model_alias+" ==========", info=False) }}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro create_tag_if_missing(all_tag_names,table_tag) %}
	{% if table_tag not in all_tag_names %}
		{{ log('Creating missing tag '+table_tag, info=False) }}
        {%- call statement('main', fetch_result=True) -%}
            create tag if not exists silver.{{table_tag}}
        {%- endcall -%}
		{{ log(load_result('main').data, info=False) }}
	{% else %}
		{{ log('Tag already exists: '+table_tag, info=False) }}
	{% endif %}
{% endmacro %}

{% macro set_table_tag_value_if_different(model_schema,table_name,tag_name,desired_tag_value,existing_tags) %}
    {{ log('Ensuring tag '+tag_name+' has value '+desired_tag_value+' at table level', info=False) }}
    {%- set existing_tag_for_table = existing_tags|selectattr('0','equalto','TABLE')|selectattr('1','equalto',tag_name|upper)|list -%}
    {{ log('Filtered tags for table:', info=False) }}
    {{ log(existing_tag_for_table[0], info=False) }}
    {% if existing_tag_for_table|length > 0 and existing_tag_for_table[0][2]==desired_tag_value %}
        {{ log('Correct tag value already exists', info=False) }}
    {% else %}
        {{ log('Setting tag value for '+tag_name+' to value '+desired_tag_value, info=False) }}
        {%- call statement('main', fetch_result=True) -%}
            alter table {{model_schema}}.{{table_name}} set tag {{tag_name}} = '{{desired_tag_value}}'
        {%- endcall -%}
        {{ log(load_result('main').data, info=False) }}
    {% endif %}
{% endmacro %}

{% macro set_column_tag_value_if_different(table_name,column_name,tag_name,desired_tag_value,existing_tags) %}
    {{ log('Ensuring tag '+tag_name+' has value '+desired_tag_value+' at column level', info=False) }}
    {%- set existing_tag_for_column = existing_tags|selectattr('0','equalto','COLUMN')|selectattr('1','equalto',tag_name|upper)|list -%}
    {{ log('Filtered tags for column:', info=False) }}
    {{ log(existing_tag_for_column[0], info=False) }}
    {% if existing_tag_for_column|length > 0 and existing_tag_for_column[0][2]==desired_tag_value %}
        {{ log('Correct tag value already exists', info=False) }}
    {% else %}
        {{ log('Setting tag value for '+tag_name+' to value '+desired_tag_value, info=False) }}
        {%- call statement('main', fetch_result=True) -%}
            alter table {{table_name}} modify column {{column_name}} set tag {{tag_name}} = '{{desired_tag_value}}'
        {%- endcall -%}
        {{ log(load_result('main').data, info=False) }}
    {% endif %}
{% endmacro %}

{% macro set_database_tag_value(tag_name,tag_value) %}
    {% set query %}
        create tag if not exists silver.{{tag_name}}
    {% endset %}
    {% do run_query(query) %}
    {% set query %}
        alter database {{target.database}} set tag {{target.database}}.silver.{{tag_name}} = '{{tag_value}}'
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}

{% macro set_schema_tag_value(target_schema,tag_name,tag_value) %}
    {% set query %}
        create tag if not exists silver.{{tag_name}}
    {% endset %}
    {% do run_query(query) %}
    {% set query %}
        alter schema {{target.database}}.{{target_schema}} set tag {{target.database}}.silver.{{tag_name}} = '{{tag_value}}'
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}