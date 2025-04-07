{{ config(
    tags = ["test_daily"]
) }}

WITH max_block AS (
    SELECT
        MAX(block_id) AS max_block_id
    FROM
        {{ ref('silver__blocks') }}
),
--We can't do a direct join because the blocks at the start of an epoch could be skipped and thus would not be in the blocks table
epoch_range AS (
    SELECT
        e.epoch,
        e.start_block,
        e.end_block
    FROM
        solana.silver.epoch e
    JOIN
        max_block mb
    ON
        mb.max_block_id BETWEEN e.start_block AND e.end_block
),

closest_block AS (
    SELECT
        e.epoch,
        MIN(b.block_id) AS closest_block_id,
        MIN(b.block_timestamp) AS start_block_timestamp
    FROM
        epoch_range e
    LEFT JOIN
        solana.silver.blocks b
    ON
        b.block_id >= e.start_block
        AND b.block_id <= e.end_block
    GROUP BY
        e.epoch
),

recent_epoch_stake_account AS (
    SELECT
        MAX(epoch_recorded) AS existing_max_epoch
    FROM
        {{ ref('silver__snapshot_stake_accounts_2') }}
)

SELECT
    *
FROM
    closest_block a
LEFT JOIN
    recent_epoch_stake_account b
ON
    a.epoch = b.existing_max_epoch
WHERE
    b.existing_max_epoch IS NULL
    AND a.start_block_timestamp <= (SYSDATE() - INTERVAL '24 HOUR')
