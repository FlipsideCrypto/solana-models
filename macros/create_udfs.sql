{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        {% if target.database != "SOLANA_COMMUNITY_DEV" %}
            {{ udf_bulk_get_decoded_instructions_data() }};
            {{ udf_bulk_get_validator_metadata() }};
            {{ udf_bulk_get_blocks() }};
            {{ udf_bulk_get_block_txs() }};
            {{ udf_snapshot_get_vote_accounts() }};
            {{ udf_snapshot_get_validators_app_data() }};
            {{ udf_snapshot_get_vote_program_accounts() }};
            {{ udf_bulk_program_parser() }};
            {{ udf_decode_instructions() }};
            {{ udf_bulk_parse_compressed_nft_mints() }};
            {{ udf_bulk_get_solscan_blocks() }};
            {{ create_udf_bulk_instructions_decoder() }};
            {{ create_udf_verify_idl() }};
            {{ create_udf_decode_compressed_mint_change_logs() }};
            {{ create_udf_bulk_rest_api_v2() }};
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
        {{
            create_udf_get_account_pubkey_by_name(
            schema = "silver"
        ) }}
        {{
            create_udf_get_logs_program_data(
            schema = "silver"
        ) }}
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
