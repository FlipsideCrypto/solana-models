{{ config(
    materialized = 'view'
) }}

SELECT
    'magic eden v1' AS marketplace,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_magic_eden_v1') }}
UNION
SELECT
    'magic eden v2',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_magic_eden_v2') }}

UNION
SELECT
    'solanart',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_solanart') }}

UNION
SELECT
    'solana monkey business marketplace',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_smb') }}

UNION
SELECT
    'solport',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_solport') }}
