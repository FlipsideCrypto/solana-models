{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    unique_key = ['fact_nft_sales_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','is_compressed','program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, purchaser, seller, mint, marketplace)'),
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(modified_timestamp) AS max_modified_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

-- Select from the deprecated _view models only during the initial FR
{% if not is_incremental() %}
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
    {{ ref('silver__nft_sales_amm_sell_view') }}
WHERE
    block_timestamp::date < '2022-10-30'
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
    nft_sales_hyperspace_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_hyperspace_view') }}
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
    {{ ref('silver__nft_sales_hadeswap_view') }}
WHERE
    block_timestamp::date <= '2023-02-08'
UNION ALL
{% endif %}
-- Only select from active models during incremental
SELECT
    'magic eden v2' as marketplace,
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
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
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
    {{ ref('silver__nft_sales_solanart') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
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
    nft_sales_hadeswap_decoded_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_hadeswap_decoded') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
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
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
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
    nft_sales_amm_sell_decoded_id as fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_amm_sell_decoded') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
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
            ['tx_id','index','inner_index','mint']
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
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
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
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
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
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
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
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
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
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
UNION ALL
SELECT
    'tensor' AS marketplace,
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
    nft_sales_tensor_bid_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__nft_sales_tensor_bid') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
