{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}}
) }}

SELECT
    *
FROM
    {{ ref('nft__fact_nft_sales') }}
