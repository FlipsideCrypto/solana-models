{{ config (
    materialized = 'view'
) }}

{% set model = "stake_program_accounts_2" %}
{{ streamline_external_table_query(
    model,
    partition_function = "split_part(file_name, '/', 3)",
    partition_name = "_partition_by_created_year",
    unique_key = "",
    other_cols = ""
) }}
