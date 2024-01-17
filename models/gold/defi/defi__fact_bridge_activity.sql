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
        bridge_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id','tx_id', 'index']
        ) }}
    ) AS fact_bridge_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__bridge_transfers') }}
