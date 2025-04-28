-- depends_on: {{ ref('bronze__streamline_block_rewards_2') }}

{{ config(
    materialized = 'incremental',
    unique_key = ["vote_pubkey", "epoch_earned", "block_id"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'floor(block_id,-6)', '_inserted_timestamp::DATE'],
    tags = ['rewards', 'scheduled_non_core']
) }}

WITH base AS (
    SELECT
        A.block_id,
        b.block_timestamp AS block_timestamp,
        A.data:commission::INTEGER AS commission,
        A.data:lamports::INTEGER AS amount,
        A.data:postBalance::INTEGER AS post_balance,
        A.data:pubkey::STRING AS account,
        A.data:rewardType::STRING AS reward_type,
        A._inserted_timestamp,
        A._partition_id
    FROM
        {% if is_incremental() %}
        {{ ref('bronze__streamline_block_rewards_2') }} A
        {% else %}
        {{ ref('bronze__streamline_FR_block_rewards_2') }} A
        {% endif %}
    LEFT OUTER JOIN 
        {{ ref('silver__blocks') }} b
        ON b.block_id = A.block_id
    WHERE
        error IS NULL
        AND reward_type = 'Voting'
        {% if is_incremental() %}
        AND _partition_id >= (SELECT MAX(_partition_id) - 1 FROM {{ this }})
        {% else %}
        AND _partition_id >= 59891
        {% endif %}
),
{% if is_incremental() %}
prev_null_block_timestamp_txs AS (
    SELECT
        b.block_timestamp,
        A.block_id,
        A.reward_amount_sol,
        A.post_balance_sol,
        A.commission,
        A.vote_pubkey,
        A.epoch_earned,
        A._partition_id,
        A.rewards_voting_2_id,
        A.epoch_id,
        A.inserted_timestamp,
        A.modified_timestamp,
        A._invocation_id,
        greatest(A._inserted_timestamp, b._inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ this }} A
    INNER JOIN 
        {{ ref('silver__blocks') }} b
        ON b.block_id = A.block_id
    WHERE
        A.block_timestamp::DATE IS NULL
        AND A.block_id > 39824213
),
{% endif %}
epoch AS (
    SELECT *
    FROM 
        {{ ref('silver__epoch') }}
    {% if is_incremental() %}
    WHERE start_block <= (SELECT MAX(block_id) FROM base)
    {% endif %}
),
pre_final AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.amount / pow(10, 9) AS reward_amount_sol,
        A.post_balance / pow(10, 9) AS post_balance_sol,
        A.commission,
        A.account AS vote_pubkey,
        (b.epoch - 1) AS epoch_earned,
        A._partition_id,
        {{ dbt_utils.generate_surrogate_key(['epoch_earned', 'a.block_id', 'a.account']) }} AS rewards_voting_2_id,
        {{ dbt_utils.generate_surrogate_key(['epoch_earned']) }} AS epoch_id,
        sysdate() AS inserted_timestamp,
        sysdate() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id,
        A._inserted_timestamp
    FROM
        base A
    LEFT JOIN 
        epoch b
        ON A.block_id BETWEEN b.start_block AND b.end_block 
    {% if is_incremental() %}
    UNION
    SELECT *
    FROM 
        prev_null_block_timestamp_txs
    {% endif %}
)
SELECT *
FROM 
    pre_final 
QUALIFY 
    row_number() OVER(
        PARTITION BY epoch_earned, vote_pubkey, block_id
        ORDER BY _inserted_timestamp DESC
    ) = 1