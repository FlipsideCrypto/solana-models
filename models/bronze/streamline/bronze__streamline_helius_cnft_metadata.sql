{{ config (
    materialized = 'view'
) }}

{% set model = "helius_nft_metadata" %}
{{ streamline_external_table_query(
    model,
    partition_function = "concat_ws('-',split_part(split_part(file_name,'/',3),'_',1),split_part(split_part(file_name,'/',3),'_',2),split_part(split_part(file_name,'/',3),'_',3))",
    partition_name = "_partition_by_created_date",
    unique_key = "data:id::STRING AS mint",
    other_cols="HELIUS_NFT_METADATA_REQUESTS_ID, MAX_MINT_EVENT_INSERTED_TIMESTAMP"
) }}