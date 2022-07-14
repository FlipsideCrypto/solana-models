{% macro udf_bulk_get_stake_account_tx_ids() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_get_stake_account_tx_ids() returns text api_integration = aws_solana_api_dev AS {% if target.name == "prod" -%}
        'https://pj4rqb8z96.execute-api.us-east-1.amazonaws.com/prod/bulk_get_stake_account_tx_ids'
    {% else %}
        'https://11zlwk4fm3.execute-api.us-east-1.amazonaws.com/dev/bulk_get_stake_account_tx_ids'
    {%- endif %}
{% endmacro %}