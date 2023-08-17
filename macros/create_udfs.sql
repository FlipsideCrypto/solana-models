{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        {% if target.database != "SOLANA_COMMUNITY_DEV" %}
            {{ udf_bulk_get_decoded_instructions_data() }};
            {{ udf_bulk_get_validator_metadata() }};
            {{ udf_bulk_get_blocks() }};
            {{ udf_bulk_get_block_txs() }};
            {{ udf_bulk_get_block_rewards() }};
            {{ udf_snapshot_get_vote_accounts() }};
            {{ udf_snapshot_get_validators_app_data() }};
            {{ udf_snapshot_get_stake_accounts() }};
            {{ udf_snapshot_get_vote_program_accounts() }};
            {{ udf_decode_instructions() }};
            {{ udf_bulk_program_parser() }};
        {% endif %}

        {{ create_udf_ordered_signers(
            schema = "silver"
        ) }}
        {{ create_udf_get_all_inner_instruction_events(
            schema = "silver"
        ) }}
        {{ create_udf_get_account_balances_index(
            schema = "silver"
        ) }}
        {{ 
            create_udf_get_all_inner_instruction_program_ids(
            schema = "silver"
        ) }}
        {{ 
            create_udf_get_multi_signers_swapper(
            schema = "silver"
        ) }}
        {{ 
            create_udf_get_jupv4_inner_programs(
            schema = "silver"
        ) }}
        {{
            create_udf_get_compute_units_consumed(
            schema = "silver"
        ) }}
        {{
            create_udf_get_compute_units_total(
            schema = "silver"
        ) }}
        {{
            create_udf_get_tx_size(
            schema = "silver"
        ) }}
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
