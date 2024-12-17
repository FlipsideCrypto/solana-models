{{ config (
    materialized = 'view'
) }}

{% set model = "helius_blocks" %}
{{ streamline_external_table_FR_query_v2(
    model,
    partition_function = "split_part(file_name, '/', 3)",
    partition_name = "_partition_by_created_date",
    unique_key = "block_id",
    other_cols="array_size(data:result:signatures::array) AS transaction_count"
) }}
