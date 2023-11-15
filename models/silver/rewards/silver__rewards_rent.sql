{{ config(
    materialized = 'incremental',
    unique_key = ["vote_pubkey","epoch_earned","block_id"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','floor(block_id,-6)','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(vote_pubkey, epoch_earned);",
    tags = ['rewards']
) }}

WITH base AS (

    SELECT
        A.block_id,
        b.block_timestamp AS block_timestamp,
        A.data :commission :: INTEGER AS commission,
        A.data :lamports :: INTEGER AS amount,
        A.data :postBalance :: INTEGER AS post_balance,
        A.data :pubkey :: STRING AS account,
        A.data :rewardType :: STRING AS reward_type,
        A._inserted_timestamp,
        A._partition_id
    FROM
        {{ ref('bronze__block_rewards') }} A
        LEFT OUTER JOIN {{ ref('silver__blocks') }}
        b
        ON b.block_id = A.block_id
    WHERE
        error IS NULL
        AND reward_type = 'Rent'

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
    AND A._partition_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(_partition_id), 1) + 1, 28780)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(_partition_id), 1) + 100, 28780)
        FROM
            {{ this }}
        ) 
{% elif is_incremental() %}
    AND _partition_id >= (
        SELECT
            MAX(_partition_id) -1
        FROM
            {{ this }}
        )
    AND _partition_id <= (
        SELECT
            MAX(_partition_id)
        FROM
        {{ source('solana_streamline','complete_block_rewards') }}
    )
{% else %}
    AND _partition_id <= 10
{% endif %}
),

{% if is_incremental() %}
prev_null_block_timestamp_txs AS (
    SELECT
    b.block_timestamp,
    A.block_id,
    A.reward_amount_sol,
    A.post_balance_sol,
    A.vote_pubkey,
    A.epoch_earned,
    A._partition_id,
    A.rewards_rent_id,
    A.epoch_id,
    A.inserted_timestamp,
    A.modified_timestamp,
    A.invocation_id,
    GREATEST(
            A._inserted_timestamp,
            b._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ this }}
        A
        INNER JOIN {{ ref('silver__blocks') }}
        b
        ON b.block_id = A.block_id
    WHERE
        A.block_timestamp :: DATE IS NULL
        AND A.block_id > 39824213
),
{% endif %}
epoch AS (
    SELECT
        *
    FROM
        {{ ref('silver__epoch') }}

{% if is_incremental() %}
WHERE
    epoch <= (
        SELECT
            MAX(epoch_earned) + 10
        FROM
            {{ this }}
    )
{% else %}
WHERE
    epoch <= 540
{% endif %}
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.amount / pow(
        10,
        9
    ) AS reward_amount_sol,
    A.post_balance / pow(
        10,
        9
    ) AS post_balance_sol,
    A.account AS vote_pubkey,
    b.epoch AS epoch_earned,
    A._partition_id,
    {{ dbt_utils.generate_surrogate_key(['b.epoch','a.block_id','a.account']) }} AS rewards_rent_id,
    {{ dbt_utils.generate_surrogate_key(['b.epoch']) }} AS epoch_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id,
    A._inserted_timestamp
FROM
    base A
    LEFT JOIN epoch b
    ON A.block_id BETWEEN b.start_block
    AND b.end_block qualify(ROW_NUMBER() over(PARTITION BY epoch_earned, account, block_id
ORDER BY
    _inserted_timestamp DESC)) = 1

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    prev_null_block_timestamp_txs
{% endif %}
