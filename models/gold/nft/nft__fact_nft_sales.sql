{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} },
    tags = ['scheduled_non_core']
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
    sales_amount,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
    
FROM
    {{ ref(
        'silver__nft_sales_magic_eden_v1_view'
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
    sales_amount,
    COALESCE (
        nft_sales_magic_eden_v2_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_nft_sales_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    sales_amount,
    COALESCE (
        nft_sales_solanart_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_nft_sales_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    seller,
    mint,
    sales_amount,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_smb_view') }}
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
    sales_amount,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref(
        'silver__nft_sales_solport_view'
    ) }}
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
    sales_amount,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_opensea_view') }}
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
    sales_amount,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_yawww_view') }}
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
    sales_amount,
    COALESCE (
        nft_sales_hadeswap_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','mint']
        ) }}
    ) AS fact_nft_sales_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    sales_amount,
    COALESCE (
        nft_sales_hyperspace_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','mint']
        ) }}
    ) AS fact_nft_sales_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    sales_amount,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','mint']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_coral_cube_view') }}
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
    sales_amount,
    COALESCE (
        nft_sales_exchange_art_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','mint']
        ) }}
    ) AS fact_nft_sales_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_exchange_art') }}
UNION
SELECT
    marketplace,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    COALESCE (
        nft_sales_amm_sell_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','mint']
        ) }}
    ) AS fact_nft_sales_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_amm_sell') }}
UNION
SELECT
    'tensorswap',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    COALESCE (
        nft_sales_tensorswap_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','mint','purchaser']
        ) }}
    ) AS fact_nft_sales_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_tensorswap') }}
UNION
SELECT
    'solsniper',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    nft_sales_solsniper_id AS fact_nft_sales_id,
    inserted_timestamp,
     modified_timestamp
FROM
    {{ ref('silver__nft_sales_solsniper') }}
