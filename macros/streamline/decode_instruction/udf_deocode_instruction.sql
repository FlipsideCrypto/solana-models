{% macro udf_decode_instruction() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_decode_instruction("JSON" ARRAY) returns VARIANT api_integration = aws_solana_api_dev AS {% if target.database == 'SOLANA' -%}
        'https://pj4rqb8z96.execute-api.us-east-1.amazonaws.com/prod/decode_instruction'
    {% else %}
        'https://89kf6gtxr0.execute-api.us-east-1.amazonaws.com/dev/decode_instruction'
    {%- endif %}
{% endmacro %}