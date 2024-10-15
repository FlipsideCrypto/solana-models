{{ config (
    materialized = 'view'
) }}

{% set model = "blocks_2" %}
{{ streamline_external_table_query_v2(
    model,
    partition_function = "split_part(file_name, '/', 3)",
    partition_name = "_partition_by_created_date",
    unique_key = "block_id",
    other_cols="error"
) }}
