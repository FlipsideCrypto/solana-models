{% macro create_udfs() %}
    {% set sql %}
    {{ udf_bulk_get_decoded_instructions_data() }};
    {{ create_udf_ordered_signers(
        schema = "silver"
    ) }}
    {{ create_udf_get_all_inner_instruction_events(
        schema = "silver"
    ) }}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
