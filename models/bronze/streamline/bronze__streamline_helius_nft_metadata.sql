{{ config (
    materialized = 'view'
) }}

{% set model = "helius_nft_metadata" %}
{{ streamline_external_table_query(
    model,
    partition_function = "to_date(split_part(split_part(file_name,'/',4),'_',1))",
    partition_name = "_partition_id",
    unique_key = "mint",
    other_cols="helius_nft_metadata_requests_id,max_mint_event_inserted_timestamp"
) }}