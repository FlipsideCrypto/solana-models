{{ config (
    materialized = 'view'
) }}

{% set model = "block_rewards_2" %}
{{ streamline_external_table_FR_query_v2(
    model,
    partition_function = "to_number(split_part(split_part(split_part(file_name,'/',3),'=',2),'_',1))",
    partition_name = "_partition_id",
    unique_key = "block_id",
    other_cols="error"
) }}
