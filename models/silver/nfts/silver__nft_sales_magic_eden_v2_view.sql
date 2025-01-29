{{ config(
  materialized = 'view'
) }}

{% set use_to_block_timestamp = '2024-03-16' %}

WITH base AS (
    SELECT
        COALESCE(d.block_timestamp, v.block_timestamp) AS block_timestamp,
        COALESCE(d.block_id, v.block_id) AS block_id,
        COALESCE(d.tx_id, v.tx_id) AS tx_id,
        COALESCE(d.succeeded, v.succeeded) AS succeeded,
        COALESCE(d.program_id, v.program_id) AS program_id,
        COALESCE(d.mint, v.mint) AS mint,
        COALESCE(d.purchaser, v.purchaser) AS purchaser,
        COALESCE(d.seller, v.seller) AS seller,
        COALESCE(d.sales_amount, v.sales_amount) AS sales_amount,
        COALESCE(d.currency_address, '{{ SOL_MINT }}') AS currency_address,
        COALESCE(d.nft_sales_magic_eden_v2_decoded_id, v.nft_sales_magic_eden_v2_id) AS nft_sales_magic_eden_v2_id,
        COALESCE(d._inserted_timestamp, v._inserted_timestamp) AS _inserted_timestamp,
        COALESCE(d.inserted_timestamp, v.inserted_timestamp, '2000-01-01') AS inserted_timestamp,
        COALESCE(d.modified_timestamp, v.modified_timestamp, '2000-01-01') AS modified_timestamp
    FROM
        {{ ref('silver__nft_sales_magic_eden_v2_decoded') }} d
    FULL OUTER JOIN
        {{ source('solana_silver', 'nft_sales_magic_eden_v2') }} v
    ON
        d.tx_id = v.tx_id
        and d.block_timestamp::date = v.block_timestamp::date
)

SELECT
    *
FROM
    base
WHERE
    block_timestamp::DATE < '{{ use_to_block_timestamp }}'
    AND succeeded