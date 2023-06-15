{% macro udf_decode_instructions() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION solana.silver.udf_decode_instructions(
        program_id STRING,
        json OBJECT
    ) returns OBJECT api_integration = aws_solana_api_dev AS 'https://2e428s20uh.execute-api.us-east-1.amazonaws.com/Prod/decode'
{% endmacro %}
