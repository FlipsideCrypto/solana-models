{{ config (
    materialized = 'view'
) }}

{% set model = "solscan_blocks_2" %}
{{ streamline_external_table_query(
    model,
    partition_function = "to_date(split_part(split_part(file_name, '/', -2), '_result', 1), 'YYYY_MM_DD')",
    partition_name = "_partition_by_created_date",
    unique_key = "BLOCK_ID",
    other_cols=""
) }}