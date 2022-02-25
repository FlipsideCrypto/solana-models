{% macro create_tasks() %}
    CREATE SCHEMA IF NOT EXISTS _internal;

    {% if target.database == 'SOLANA' %}
        {{ task_run_sp_create_prod_clone('_internal') }};
    {% endif %}

{% endmacro %}