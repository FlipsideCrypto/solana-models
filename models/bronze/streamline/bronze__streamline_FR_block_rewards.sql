{{ config (
    materialized = 'view'
) }}

{% set model = "block_rewards" %}
{{ streamline_external_table_FR_query(
    model,
    partition_function = "to_number(split_part(split_part(file_name,'/',3),'=',2))",
    partition_name = "_partition_id",
    unique_key = "block_id",
    other_cols="error"
) }}
