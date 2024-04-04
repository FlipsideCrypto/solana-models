{{ config(
    materialized = 'table',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(account_address,owner);",
    tags = ['daily']
) }}
/* need to rebucket and regroup the intermediate model due to possibility of change events coming in out of order */
WITH rebucket AS (

    SELECT
        account_address,
        owner,
        start_block_id,
        conditional_change_event(owner) over (
            PARTITION BY account_address
            ORDER BY
                start_block_id
        ) AS bucket
    FROM
        {{ ref('silver__token_account_owners_intermediate') }}
),
regroup AS (
    SELECT
        account_address,
        owner,
        bucket,
        MIN(start_block_id) AS start_block_id
    FROM
        rebucket
    GROUP BY
        account_address,
        owner,
        bucket
),
pre_final AS (
    SELECT
        account_address,
        owner,
        start_block_id,
        LEAD(start_block_id) ignore nulls over (
            PARTITION BY account_address
            ORDER BY
                bucket
        ) AS end_block_id
    FROM
        regroup
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['account_address','start_block_id']
    ) }} AS token_account_owners_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
WHERE
    start_block_id <> end_block_id
    OR end_block_id IS NULL
