{% macro udf_snapshot_get_stake_accounts() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_snapshot_get_stake_accounts() returns text api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://pj4rqb8z96.execute-api.us-east-1.amazonaws.com/prod/bulk_snapshot_get_stake_program_accounts'
    {% else %}
        'https://11zlwk4fm3.execute-api.us-east-1.amazonaws.com/dev/bulk_snapshot_get_stake_program_accounts'
    {%- endif %}
{% endmacro %}