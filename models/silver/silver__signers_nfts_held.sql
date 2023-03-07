{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', signer, nft_held)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'signer'
) }} 
WITH dates_changed AS (
    SELECT
        DISTINCT block_timestamp :: DATE AS block_timestamp_date
    FROM
        {{ ref('silver__transactions') }}
    WHERE 
        succeeded
),
pre_final AS (
    SELECT
        signer,
        token_in as NFT_held
    FROM
        {{ ref('silver___signers_nfts_in') }}

    EXCEPT

    SELECT 
        signer, 
        token_out as NFT_held
    FROM 
        {{ ref('silver___signers_nfts_out') }}
)
SELECT 
    p.signer, 
    NFT_held
FROM 
    pre_final p

WHERE 
    NFT_held IS NOT NULL
    AND signer IS NOT NULL
GROUP BY 
    p.signer, 
    NFT_held