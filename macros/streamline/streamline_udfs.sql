{% macro create_udf_bulk_solana_decoder() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_solana_decoder(
        json variant
    ) returns text api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://7938mznoq8.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_solana_decoder'
    {% else %}
        'https://7938mznoq8.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_solana_decoder'
    {%- endif %}
{% endmacro %}
