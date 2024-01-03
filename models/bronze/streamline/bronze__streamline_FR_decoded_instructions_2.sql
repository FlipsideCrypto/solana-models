{{ config (
    materialized = 'view'
) }}

{% set model = "decoded_instructions_2" %}
{{ streamline_external_table_FR_query(
    model,
    partition_function = "to_date(concat_ws('-', split_part(file_name, '/', 3),split_part(file_name, '/', 4), split_part(file_name, '/', 5)))",
    partition_name = "_partition_by_created_date",
    unique_key = "block_id",
    other_cols = "tx_id,index,inner_index,program_id,_partition_by_block_id"
) }}
