{{ config (
    materialized = 'view'
) }}

{% set model = "block_txs_index_backfill" %}
{{ streamline_external_table_query_v2(
    model,
    partition_function = "to_date(split_part(split_part(file_name, '/', -2), '_result', 1), 'YYYY_MM_DD')",
    partition_name = "_partition_by_created_date",
    unique_key = "block_id",
    other_cols=""
) }}
