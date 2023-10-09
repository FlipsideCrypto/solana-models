{% macro udf_bulk_get_blocks_tx_count() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_get_blocks_tx_count("JSON" ARRAY) returns VARIANT api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://pj4rqb8z96.execute-api.us-east-1.amazonaws.com/prod/bulk_get_block_tx_counts'
    {% else %}
        'https://89kf6gtxr0.execute-api.us-east-1.amazonaws.com/dev/bulk_get_block_tx_counts'
    {%- endif %}
{% endmacro %}