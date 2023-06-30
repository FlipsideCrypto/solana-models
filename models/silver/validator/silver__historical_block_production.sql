{{ config (
    materialized = 'view'
) }}

SELECT
    a.json_data :metadata :epoch AS epoch,
    f.value :identityPubkey :: STRING AS node_pubkey,
    f.value :leaderSlots :: INT AS num_leader_slots,
    f.value :blocksProduced :: INT AS num_blocks_produced,
    a.json_data :metadata :start_slot :: INT AS start_slot,
    a.json_data :metadata :end_slot :: INT AS end_slot
FROM
    solana_dev.bronze.block_production a,
    LATERAL FLATTEN(
        input => a.json_data :metadata :leaders
    ) AS f
group by 1,2,3,4,5,6
