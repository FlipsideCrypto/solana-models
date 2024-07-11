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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
    
FROM
    {{ ref(
        'silver__nft_sales_magic_eden_v1_view'
    ) }}
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
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
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
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
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_smb_view') }}
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref(
        'silver__nft_sales_solport_view'
    ) }}
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_opensea_view') }}
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_yawww_view') }}
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
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
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
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
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','mint']
    ) }} AS fact_nft_sales_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_coral_cube_view') }}
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
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
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
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
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
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
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    nft_sales_solsniper_id AS fact_nft_sales_id,
    inserted_timestamp,
     modified_timestamp
FROM
    {{ ref('silver__nft_sales_solsniper') }}
UNION ALL
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
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    nft_sales_solsniper_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__nft_sales_solsniper_v1_events_view') }}
UNION ALL
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
    tree_authority,
    merkle_tree,
    leaf_index,
    TRUE as is_compressed,
    nft_sales_tensorswap_cnft_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__nft_sales_tensorswap_cnft') }}
UNION ALL
SELECT
    'magic eden v3' AS marketplace,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    tree_authority,
    merkle_tree,
    leaf_index,
    TRUE as is_compressed,
    nft_sales_magic_eden_cnft_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__nft_sales_magic_eden_cnft') }}
UNION ALL
SELECT
    'solsniper' AS marketplace,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    tree_authority,
    merkle_tree,
    leaf_index,
    TRUE as is_compressed,
    nft_sales_solsniper_cnft_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__nft_sales_solsniper_cnft') }}
