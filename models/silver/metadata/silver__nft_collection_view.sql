{{
    config(
        materialized = 'view',
    )
}}

SELECT *
FROM
    {{ source('solana_silver', 'nft_collection') }}