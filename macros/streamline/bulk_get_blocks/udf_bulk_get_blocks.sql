{% macro udf_bulk_get_blocks() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_get_blocks(is_real_time boolean) returns text api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://pj4rqb8z96.execute-api.us-east-1.amazonaws.com/prod/bulk_get_blocks'
    {% else %}
        'https://11zlwk4fm3.execute-api.us-east-1.amazonaws.com/dev/bulk_get_blocks'
    {%- endif %}
{% endmacro %}