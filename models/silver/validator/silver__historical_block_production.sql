{{ config(
    materialized = 'view',
    tags = ['validator_historical']
) }}

SELECT
    a.json_data:metadata:epoch AS epoch,
    f.value:identityPubkey::STRING AS node_pubkey,
    f.value:leaderSlots::INT AS num_leader_slots,
    f.value:blocksProduced::INT AS num_blocks_produced,
    a.json_data:metadata:start_slot::INT AS start_slot,
    a.json_data:metadata:end_slot::INT AS end_slot
FROM
    {{ source('bronze', 'block_production') }} a,
    LATERAL FLATTEN(input => a.json_data:metadata:leaders) AS f
GROUP BY
    1, 2, 3, 4, 5, 6
QUALIFY
    row_number() OVER (
        PARTITION BY epoch, node_pubkey
        ORDER BY epoch DESC
    ) = 1

UNION ALL

SELECT
    epoch,
    node_pubkey,
    num_leader_slots,
    num_blocks_produced,
    start_slot,
    end_slot
FROM
    {{ ref('silver__snapshot_block_production_view') }}
WHERE
    epoch BETWEEN 469 AND 552