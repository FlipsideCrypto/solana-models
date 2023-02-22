{{ config(
    materialized = 'view'
) }}

SELECT
    signer,
    NFT_held
FROM
    {{ ref('silver__signers_nfts_held') }}