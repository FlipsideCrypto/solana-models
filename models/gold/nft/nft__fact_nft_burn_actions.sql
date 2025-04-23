{{ config(
    materialized = 'incremental',
    unique_key = ['fact_nft_burn_actions_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','ROUND(block_id, -3)', 'event_type'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, mint, burn_authority)'),
    full_refresh = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_modified_query %}
    SELECT
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query)[0][0] %}
    {% endif %}
{% endif %}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    mint,
    burn_amount,
    burn_authority,
    signers,
    DECIMAL,
    MINT_STANDARD_TYPE,
    COALESCE (
        nft_burn_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'index', 'inner_index', 'mint']
        ) }}
    ) AS fact_nft_burn_actions_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    {{ ref('silver__nft_burn_actions') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
