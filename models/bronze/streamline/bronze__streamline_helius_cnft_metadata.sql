{{ config (
    materialized = 'view'
) }}

{% set model = "helius_nft_metadata" %}
{{ streamline_external_table_query(
    model,
    partition_function = "to_timestamp(split_part(split_part(file_name, '/', -2), '_result', 1), 'YYYY_MM_DD_HH24')",
    partition_name = "_partition_by_created_hour",
    unique_key = "data:id::STRING AS mint",
    other_cols="HELIUS_NFT_METADATA_REQUESTS_ID, MAX_MINT_EVENT_INSERTED_TIMESTAMP"
) }}