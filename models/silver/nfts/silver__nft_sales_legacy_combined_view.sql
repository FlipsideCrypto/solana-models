{{ config(
  materialized = 'view'
) }}

{% if execute %}

    {% set SOL_MINT = 'So11111111111111111111111111111111111111111' %}

{% endif %}

SELECT
    'magic eden v1' AS marketplace,
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint']
    ) }} AS nft_sales_legacy_combined_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref(
        'silver__nft_sales_magic_eden_v1_view'
    ) }}
UNION ALL
SELECT
    'solana monkey business marketplace',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS nft_sales_legacy_combined_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_smb_view') }}
UNION ALL
SELECT
    'solport',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint']
    ) }} AS nft_sales_legacy_combined_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref(
        'silver__nft_sales_solport_view'
    ) }}
UNION ALL
SELECT
    'opensea',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'mint']
    ) }} AS nft_sales_legacy_combined_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_opensea_view') }}
UNION ALL
SELECT
    'yawww',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS nft_sales_legacy_combined_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_yawww_view') }}
UNION ALL
SELECT
    'coral cube',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','mint']
    ) }} AS nft_sales_legacy_combined_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_coral_cube_view') }}
UNION ALL
SELECT
    marketplace,
    case when marketplace = 'Magic Eden' then 'v2 AMM' else 'v1 AMM' end as marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    COALESCE (
        nft_sales_amm_sell_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','mint']
        ) }}
    ) AS nft_sales_legacy_combined_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_amm_sell_view') }}
WHERE
    block_timestamp::date < '2022-10-30' -- use new model after this date
UNION ALL
SELECT
    'solsniper',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    nft_sales_solsniper_id AS nft_sales_legacy_combined_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__nft_sales_solsniper_v1_events_view') }}
UNION ALL
SELECT
    'hyperspace',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    COALESCE (
         nft_sales_hyperspace_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'mint']
        ) }}
    ) AS nft_sales_legacy_combined_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_hyperspace_view') }}
UNION ALL
SELECT
    'hadeswap',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    COALESCE (
        nft_sales_hadeswap_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','mint']
        ) }}
    ) AS nft_sales_legacy_combined_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_hadeswap_view') }}
WHERE
    block_timestamp::date <= '2023-02-08' -- use new model after this date
UNION ALL
SELECT
    'solanart',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    nft_sales_solanart_id AS nft_sales_legacy_combined_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_solanart_view') }}
UNION ALL
SELECT
    'solsniper',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' as currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    nft_sales_solsniper_id AS nft_sales_legacy_combined_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_solsniper_view') }}
UNION ALL
SELECT
    'solsniper',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' AS currency_address,
    tree_authority,
    merkle_tree,
    leaf_index,
    TRUE as is_compressed,
    nft_sales_solsniper_cnft_id AS nft_sales_legacy_combined_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_solsniper_cnft_view') }}
UNION ALL
SELECT
    'tensorswap',
    'v1' AS marketplace_version,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    '{{ SOL_MINT }}' AS currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    nft_sales_tensorswap_id AS nft_sales_legacy_combined_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_tensorswap_view') }}


