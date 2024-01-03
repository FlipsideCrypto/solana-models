{% macro udf_bulk_get_solscan_blocks() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_get_solscan_blocks(url string, endpoint string, headers object, block_ids ARRAY) returns VARIANT api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://pj4rqb8z96.execute-api.us-east-1.amazonaws.com/prod/bulk_get_solscan_blocks'
    {% else %}
        'https://11zlwk4fm3.execute-api.us-east-1.amazonaws.com/dev/bulk_get_solscan_blocks'
    {%- endif %}
{% endmacro %}