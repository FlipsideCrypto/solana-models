{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}}
) }}

SELECT
    'magic eden v1' AS marketplace,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount
FROM
    {{ source(
        'solana_silver',
        'nft_sales_magic_eden_v1'
    ) }}
UNION
SELECT
    'magic eden v2',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
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
    seller,
    mint,
    sales_amount
FROM
    {{ source(
        'solana_silver',
        'nft_sales_solanart'
    ) }}
UNION
SELECT
    'solana monkey business marketplace',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
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
    seller,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_solport') }}
UNION
SELECT
    'opensea',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_opensea') }}
UNION
SELECT
    'yawww',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_yawww') }}
UNION
SELECT
    'hadeswap',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_hadeswap') }}
UNION
SELECT
    'hyperspace',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_hyperspace') }}
UNION
SELECT
    'coral cube',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_coral_cube') }}
UNION
SELECT
    'exchange art',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount
FROM
    {{ ref('silver__nft_sales_exchange_art') }}
