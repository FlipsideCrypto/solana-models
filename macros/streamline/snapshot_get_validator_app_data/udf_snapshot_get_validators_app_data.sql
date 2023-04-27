{% macro udf_snapshot_get_validators_app_data() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_snapshot_get_validators_app_data() returns text api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://pj4rqb8z96.execute-api.us-east-1.amazonaws.com/prod/snapshot_get_validators_list'
    {% else %}
        'https://11zlwk4fm3.execute-api.us-east-1.amazonaws.com/dev/snapshot_get_validators_list'
    {%- endif %}
{% endmacro %}