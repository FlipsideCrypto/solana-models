{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","account","reward_type"],
    cluster_by = ['block_timestamp::DATE','block_id','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['rewards']
) }}

WITH pre_final AS (

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
{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    a._partition_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(_partition_id), 1)+1,28780)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(_partition_id), 1)+25,28780)
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
    AND _partition_id IN (
        1,2
    )
{% endif %}
)
SELECT
    block_timestamp,
    block_id,
    amount,
    commission,
    post_balance,
    account,
    reward_type,
    _partition_id,
    _inserted_timestamp
FROM
    pre_final 
    qualify(ROW_NUMBER() over(PARTITION BY block_id, account, reward_type
ORDER BY
    _inserted_timestamp DESC)) = 1
