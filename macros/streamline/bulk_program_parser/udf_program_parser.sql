{% macro udf_bulk_program_parser() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_program_parser() returns ARRAY api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://pj4rqb8z96.execute-api.us-east-1.amazonaws.com/prod/bulk_program_parser'
    {% else %}
        'https://89kf6gtxr0.execute-api.us-east-1.amazonaws.com/dev/bulk_program_parser'
    {%- endif %}
{% endmacro %}