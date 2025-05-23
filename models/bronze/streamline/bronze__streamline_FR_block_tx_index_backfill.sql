{{ config (
    materialized = 'view'
) }}

{% set model = "block_txs_index_backfill" %}
{{ streamline_external_table_FR_query_v2(
    model,
    partition_function = "to_timestamp_ntz(split_part(split_part(file_name, '/', -2), '_result', 1), 'YYYY_MM_DD_HH24_MI')",
    partition_name = "_partition_by_created_timestamp",
    unique_key = "block_id",
    other_cols=""
) }}
