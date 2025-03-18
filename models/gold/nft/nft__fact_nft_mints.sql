{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    unique_key = ['fact_nft_mints_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, purchaser, mint)'),
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

SELECT
    block_timestamp,
    block_id,
    initialization_tx_id AS tx_id,
    succeeded,
    program_id,
    purchaser,
    mint_price,
    mint_currency,
    mint,
    FALSE AS is_compressed,
    COALESCE (
        nft_mints_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'mint', 'purchaser', 'mint_currency']
        ) }}
    ) AS fact_nft_mints_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_mints') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    mint_price,
    mint_currency,
    mint,
    TRUE AS is_compressed,
    COALESCE (
        nft_compressed_mints_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','mint']
        ) }}
    ) AS fact_nft_mints_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_compressed_mints') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
