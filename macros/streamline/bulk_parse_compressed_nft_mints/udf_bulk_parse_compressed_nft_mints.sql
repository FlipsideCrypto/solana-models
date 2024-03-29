{% macro udf_bulk_parse_compressed_nft_mints() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_parse_compressed_nft_mints("JSON" ARRAY) 
    returns ARRAY 
    api_integration = aws_solana_api_dev 
    max_batch_rows = 1
    AS {% if target.database == 'SOLANA' -%}
        'https://cpzzn7ohu0.execute-api.us-east-1.amazonaws.com/prod/parse'
    {% else %}
        'https://rd7pddtgl9.execute-api.us-east-1.amazonaws.com/dev/parse'
    {%- endif %}
{% endmacro %}