{{ config (
    materialized = 'view'
) }}

{% set model = "decoded_logs" %}
{{ streamline_external_table_query(
    model,
    partition_function = "to_timestamp_ntz(concat(split_part(file_name, '/', 3),'-',split_part(file_name, '/', 4),'-',split_part(file_name, '/', 5),' ',split_part(file_name, '/', 6),':00:00.000'))",
    partition_name = "_partition_by_created_date_hour",
    unique_key = "block_id",
    other_cols = "tx_id,index,inner_index,log_index,program_id,_partition_by_block_id"
) }}
