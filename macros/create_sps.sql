{% macro create_sps() %}
    CREATE SCHEMA IF NOT EXISTS _internal;

    {% if target.database == 'SOLANA' %}
        {{ sp_create_prod_clone('_internal') }};
    {% endif %}
{% endmacro %}