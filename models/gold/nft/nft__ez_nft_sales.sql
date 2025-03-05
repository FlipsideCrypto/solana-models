{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    unique_key = ['ez_nft_sales_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','is_compressed','program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, buyer_address, seller_address, mint, marketplace)'),
    tags = ['scheduled_non_core']
) }}

{% if execute %}

    {% set SOL_MINT = 'So11111111111111111111111111111111111111111' %}
    {% set magic_eden_switchover_block_timestamp = '2024-03-16' %}

    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(modified_timestamp) AS max_modified_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

{% set standard_platforms = [
    {'name': 'solanart', 'marketplace': 'solanart','marketplace_version': 'v1'},
    {'name': 'hadeswap_decoded', 'marketplace': 'hadeswap','marketplace_version': 'v1'},
    {'name': 'exchange_art', 'marketplace': 'exchange art', 'marketplace_version': 'v1'},
    {'name': 'amm_sell_decoded', 'marketplace': 'magic eden v2', 'marketplace_version': 'v2'},
    {'name': 'tensorswap', 'marketplace': 'tensorswap', 'marketplace_version': 'v1'},
    {'name': 'solsniper', 'marketplace': 'solsniper', 'marketplace_version': 'v1'},
    {'name': 'tensor_bid', 'marketplace': 'tensorswap', 'marketplace_version': 'v1'},
] %}

{% set cnft_platforms = [
    {'name': 'solsniper_cnft', 'marketplace': 'solsniper','marketplace_version': 'v1'},
    {'name': 'tensorswap_cnft', 'marketplace': 'tensorswap', 'marketplace_version': 'v1'},
    {'name': 'magic_eden_cnft', 'marketplace': 'magic eden v3', 'marketplace_version': 'v3'},
] %}

{% set multi_token_platforms = [
    {'name': 'magic_eden_v2_decoded', 'marketplace': 'magic eden v2', 'marketplace_version': 'v2'}
] %}

With base_nft_sales as (
-- Only select from the deprecated model during the initial FR
{% if not is_incremental() %}
SELECT
    marketplace,
    marketplace_version,
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
    currency_address,
    NULL as tree_authority,
    NULL as merkle_tree,
    NULL as leaf_index,
    FALSE as is_compressed,
    nft_sales_legacy_combined_id as ez_nft_sales_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__nft_sales_legacy_combined_view'
    ) }}
    WHERE 
        succeeded
UNION ALL
{% endif %}
-- Only select from active models during incremental
    {% for platform in standard_platforms %}
        SELECT
            '{{ platform.marketplace }}' AS marketplace,
            '{{ platform.marketplace_version }}'  AS marketplace_version,
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
            nft_sales_{{ platform.name }}_id AS ez_nft_sales_id,
            COALESCE(inserted_timestamp, '2000-01-01') AS inserted_timestamp,
            COALESCE(modified_timestamp, '2000-01-01') AS modified_timestamp
        FROM
            {{ ref('silver__nft_sales_' ~ platform.name) }}
        WHERE 
            succeeded
        {% if is_incremental() %}
        AND modified_timestamp >= '{{ max_modified_timestamp }}'
        {% endif %}

        union all
    {% endfor %}
    {% for platform in cnft_platforms %}
        SELECT
            '{{ platform.marketplace }}' AS marketplace,
            '{{ platform.marketplace_version }}'  AS marketplace_version,
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
            nft_sales_{{ platform.name }}_id AS ez_nft_sales_id,
            inserted_timestamp,
            modified_timestamp
        FROM
            {{ ref('silver__nft_sales_' ~ platform.name) }}
        WHERE 
            succeeded
        {% if is_incremental() %}
        AND modified_timestamp >= '{{ max_modified_timestamp }}'
        {% endif %}
        union all
    {% endfor %}

    {% for platform in multi_token_platforms %}
        SELECT
            '{{ platform.marketplace }}' AS marketplace,
            '{{ platform.marketplace_version }}'  AS marketplace_version,
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
            currency_address,
            NULL as tree_authority,
            NULL as merkle_tree,
            NULL as leaf_index,
            FALSE as is_compressed,
            nft_sales_{{ platform.name }}_id AS ez_nft_sales_id,
            inserted_timestamp,
            modified_timestamp
        FROM
            {{ ref('silver__nft_sales_' ~ platform.name) }}
        WHERE 
            succeeded
        {% if is_incremental() %}
        AND
            modified_timestamp >= '{{ max_modified_timestamp }}'
        {% else %}
        AND
            block_timestamp::date >= '{{magic_eden_switchover_block_timestamp}}'
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT
    a.marketplace,
    a.marketplace_version,
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    a.succeeded,
    a.index,
    a.inner_index,
    a.program_id,
    a.purchaser AS buyer_address,
    a.seller AS seller_address,
    a.mint,
    b.nft_name,
    a.sales_amount AS price,
    a.currency_address,
    c.symbol AS currency_symbol,
    (c.price * a.sales_amount) AS price_usd,
    a.tree_authority,
    a.merkle_tree,
    a.leaf_index,
    a.is_compressed,
    b.nft_collection_name,
    b.collection_id,
    b.creators,
    b.authority,
    b.metadata,
    b.image_url,
    b.metadata_uri,
    a.ez_nft_sales_id,
    a.inserted_timestamp,
    a.modified_timestamp
FROM
    base_nft_sales a
LEFT JOIN
    {{ ref('nft__dim_nft_metadata') }} b
    ON a.mint = b.mint
LEFT JOIN
    {{ ref('price__ez_prices_hourly') }} c
    ON (
        CASE 
            WHEN a.currency_address = 'So11111111111111111111111111111111111111111' THEN 'So11111111111111111111111111111111111111112'
            ELSE a.currency_address
        END
    ) = c.token_address
    AND DATE_TRUNC('hour', a.block_timestamp) = c.hour