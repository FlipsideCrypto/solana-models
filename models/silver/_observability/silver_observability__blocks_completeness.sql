{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false,
    tags = ['observability']
) }}

WITH source AS (

    SELECT
        block_id,
        block_timestamp,
        previous_BLOCK_ID AS true_prev_block_id,
        LAG(
            block_id,
            1
        ) over (
            ORDER BY
                block_id ASC
        ) AS prev_BLOCK_ID
    FROM
        {{ ref('silver__blocks') }} A
    WHERE
        block_timestamp < DATEADD(
            HOUR,
            -24,
            SYSDATE()
        )

{% if is_incremental() %}
AND (
    block_timestamp >= DATEADD(
        HOUR,
        -96,(
            SELECT
                MAX(
                    max_block_timestamp
                )
            FROM
                {{ this }}
        )
    )
    OR ({% if var('OBSERV_FULL_TEST') %}
        block_id > 39824213 --some anomalies before tx's start having block_timestamps
    {% else %}
        block_id >= (
    SELECT
        MIN(VALUE) - 1
    FROM
        (
    SELECT
        blocks_impacted_array
    FROM
        {{ this }}
        qualify ROW_NUMBER() over (
    ORDER BY
        test_timestamp DESC) = 1), LATERAL FLATTEN(input => blocks_impacted_array))
    {% endif %})
)
{% endif %}
),
block_gen AS (
    SELECT
        _id AS block_id
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id BETWEEN (
            SELECT
                MIN(block_id)
            FROM
                source
        )
        AND (
            SELECT
                MAX(block_id)
            FROM
                source
        )
)
SELECT
    'blocks' AS test_name,
    MIN(
        b.block_id
    ) AS min_block,
    MAX(
        b.block_id
    ) AS max_block,
    MIN(
        b.block_timestamp
    ) AS min_block_timestamp,
    MAX(
        b.block_timestamp
    ) AS max_block_timestamp,
    COUNT(1) AS blocks_tested,
    COUNT(
        CASE
            WHEN C.block_id IS NOT NULL THEN A.block_id
        END
    ) AS blocks_impacted_count,
    ARRAY_AGG(
        CASE
            WHEN C.block_id IS NOT NULL THEN A.block_id
        END
    ) within GROUP (
        ORDER BY
            A.block_id
    ) AS blocks_impacted_array,
    ARRAY_AGG(
        DISTINCT CASE
            WHEN C.block_id IS NOT NULL THEN OBJECT_CONSTRUCT(
                'prev_block_id',
                C.prev_block_id,
                'block_id',
                C.block_id
            )
        END
    ) AS test_failure_details,
    SYSDATE() AS test_timestamp
FROM
    block_gen A
    LEFT JOIN source b
    ON A.block_id = b.block_id
    LEFT JOIN source C
    ON A.block_id > C.prev_BLOCK_ID
    AND A.block_id < C.block_id
    AND C.block_id - C.prev_BLOCK_ID <> 1
    AND A.block_id = C.true_prev_BLOCK_ID
WHERE
    COALESCE(
        b.prev_block_id,
        C.prev_block_id
    ) IS NOT NULL
