{{ config(
    materialized = 'view',
    tags = ['streamline'],
    enabled = false,
) }}

WITH pre_final AS (

    SELECT
        SEQ8()+
        iff(
            (select max(block_id)-1000000 from {{ ref('streamline__complete_block_rewards') }}) < 148693779,
            148693779,
            (select max(block_id)-1000000 from {{ ref('streamline__complete_block_rewards') }})) 
        as block_id
    FROM
        TABLE(GENERATOR(rowcount => 5000000))
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_rewards') }}
),
gaps AS (
    SELECT
        r.*
    FROM
        {{ source('solana_test_silver', 'rewards_gaps') }} r
    LEFT JOIN
        {{ ref('streamline__complete_block_rewards') }} c
        USING(block_id)
    WHERE
        c.block_id IS NULL
),
first_epoch_block_gaps AS (
    SELECT
        block_id
    FROM
        gaps
    WHERE
        is_epoch_first_block
    ORDER BY
        block_id DESC
    LIMIT 1
),
other_gaps AS (
    SELECT
        block_id
    FROM
        gaps
    WHERE
        NOT is_epoch_first_block
    LIMIT 1000
)
SELECT
    block_id,
    (
        SELECT
            coalesce(MAX(_partition_id) + 1,1)
        FROM
            {{ ref('streamline__complete_block_rewards') }}
    ) AS batch_id
FROM
    pre_final
UNION ALL
SELECT
    block_id,
    (
        SELECT
            coalesce(MAX(_partition_id) + 1,1)
        FROM
            {{ ref('streamline__complete_block_rewards') }}
    ) AS batch_id
FROM
    first_epoch_block_gaps
UNION ALL
SELECT
    block_id,
    (
        SELECT
            coalesce(MAX(_partition_id) + 1,1)
        FROM
            {{ ref('streamline__complete_block_rewards') }}
    ) AS batch_id
FROM
    other_gaps