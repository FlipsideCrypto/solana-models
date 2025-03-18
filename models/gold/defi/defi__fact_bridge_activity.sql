{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }}},
    unique_key = ['fact_bridge_activity_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id,user_address)'),
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
    tx_id,
    succeeded,
    INDEX,
    program_id,
    platform,
    direction,
    user_address,
    amount,
    mint,
    COALESCE (
        bridge_wormhole_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id','tx_id', 'index','direction']
        ) }}
    ) AS fact_bridge_activity_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__bridge_wormhole_transfers') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
union all
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    program_id,
    platform,
    direction,
    user_address,
    amount,
    mint,
    COALESCE (
        bridge_debridge_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id','tx_id', 'index']
        ) }}
    ) AS fact_bridge_activity_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__bridge_debridge_transfers') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
union all
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    program_id,
    platform,
    direction,
    user_address,
    amount,
    mint,
    bridge_mayan_transfers_decoded_id AS fact_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__bridge_mayan_transfers_decoded') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
{% if not is_incremental() %}
union all
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    program_id,
    platform,
    direction,
    user_address,
    amount,
    mint,
    bridge_mayan_transfers_id AS fact_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__bridge_mayan_transfers_view') }}
{% endif %}