{% macro create_udfs() %}
    {% set sql %}
    {{ udf_bulk_get_decoded_instructions_data() }};
    {{ udf_bulk_get_validator_metadata() }};
    {{ udf_bulk_get_stake_account_tx_ids() }};
    {{ udf_bulk_get_txs() }};
    {{ create_udf_ordered_signers(
        schema = "silver"
    ) }}
    {{ create_udf_get_all_inner_instruction_events(
        schema = "silver"
    ) }}
    {{ create_udf_get_account_balances_index(
        schema = "silver"
    ) }}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
