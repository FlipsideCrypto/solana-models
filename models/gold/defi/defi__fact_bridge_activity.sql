{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }} },
    tags = ["scheduled_non_core"],
) }}

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