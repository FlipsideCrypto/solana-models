{% macro udf_bulk_get_decoded_instructions_data() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_get_decoded_instructions_data() returns text api_integration = aws_solana_api_dev AS {% if target.name == "prod" -%}
        'https://pj4rqb8z96.execute-api.us-east-1.amazonaws.com/prod/bulk_decode_instruction_arguments'
    {% else %}
        'https://11zlwk4fm3.execute-api.us-east-1.amazonaws.com/dev/bulk_decode_instruction_arguments'
    {%- endif %}
{% endmacro %}