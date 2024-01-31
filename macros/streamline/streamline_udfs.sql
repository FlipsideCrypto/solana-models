{% macro create_udf_bulk_instructions_decoder() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_instructions_decoder(
        json variant
    ) returns text api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://l426aqju0g.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_instructions_decoder'
    {% else %}
        'https://7938mznoq8.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_instructions_decoder'
    {%- endif %}
{% endmacro %}

{% macro create_udf_verify_idl() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_verify_idl("JSON" ARRAY) returns VARIANT api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://l426aqju0g.execute-api.us-east-1.amazonaws.com/prod/verify_idl'
    {% else %}
        'https://7938mznoq8.execute-api.us-east-1.amazonaws.com/dev/verify_idl'
    {%- endif %}
{% endmacro %}

{% macro create_udf_decode_compressed_mint_change_logs() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_decode_compressed_mint_change_logs("JSON" ARRAY) returns VARIANT api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://l426aqju0g.execute-api.us-east-1.amazonaws.com/prod/udf_decode_compressed_mint_change_logs'
    {% else %}
        'https://7938mznoq8.execute-api.us-east-1.amazonaws.com/dev/udf_decode_compressed_mint_change_logs'
    {%- endif %}
{% endmacro %}