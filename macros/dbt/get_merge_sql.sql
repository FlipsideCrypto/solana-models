{% macro get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) %}
  {{ return(adapter.dispatch('get_merge_update_columns', 'dbt')(merge_update_columns, merge_exclude_columns, dest_columns)) }}
{% endmacro %}

{% macro default__get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) %}
  {%- set default_cols = dest_columns | map(attribute="quoted") | list -%}

  {%- if merge_update_columns and merge_exclude_columns -%}
    {{ exceptions.raise_compiler_error(
        'Model cannot specify merge_update_columns and merge_exclude_columns. Please update model to use only one config'
    )}}
  {%- elif merge_update_columns -%}
    {%- set update_columns = merge_update_columns -%}
  {%- elif merge_exclude_columns -%}
    {%- set update_columns = [] -%}
    {%- for column in dest_columns -%}
      {% if column.column | lower not in merge_exclude_columns | map("lower") | list %}
        {%- do update_columns.append(column.quoted) -%}
      {% endif %}
    {%- endfor -%}
  {%- else -%}
    {%- set update_columns = default_cols -%}
  {%- endif -%}

  {{ return(update_columns) }}

{% endmacro %}

{% macro snowflake__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if config.get('merge_predicates') is none else config.get('merge_predicates') -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {% endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

    {% if unique_key %}
    when matched then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}