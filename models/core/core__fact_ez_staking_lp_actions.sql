{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base_staking_lp_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__staking_lp_actions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
merges_and_splits AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        signers,
        instruction :parsed :info :stakeAccount :: STRING AS stake_account,
        'split_source' AS event_type,
        account_keys,
        pre_balances,
        post_balances,
        instruction
    FROM
        base_staking_lp_actions
    WHERE
        event_type = 'split'
    UNION
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        signers,
        instruction :parsed :info :newSplitAccount :: STRING AS stake_account,
        'split_destination' AS event_type,
        account_keys,
        pre_balances,
        post_balances,
        instruction
    FROM
        base_staking_lp_actions
    WHERE
        event_type = 'split'
    UNION
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        signers,
        instruction :parsed :info :destination :: STRING AS stake_account,
        'merge_destination' AS event_type,
        account_keys,
        pre_balances,
        post_balances,
        instruction
    FROM
        base_staking_lp_actions
    WHERE
        event_type = 'merge'
    UNION
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        signers,
        instruction :parsed :info :source :: STRING AS stake_account,
        'merge_source' AS event_type,
        account_keys,
        pre_balances,
        post_balances,
        instruction
    FROM
        base_staking_lp_actions
    WHERE
        event_type = 'merge'
),
all_actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        signers,
        instruction :parsed :info :stakeAccount :: STRING AS stake_account,
        event_type,
        account_keys,
        pre_balances,
        post_balances,
        instruction
    FROM
        base_staking_lp_actions
    WHERE
        event_type NOT IN (
            'merge',
            'split'
        )
    UNION
    SELECT
        *
    FROM
        merges_and_splits
),
tx_base AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        event_type,
        signers,
        instruction :parsed :info :stakeAuthority :: STRING AS stake_authority,
        stake_account AS stake_acct,
        silver.udf_get_account_balances_index(
            stake_account,
            account_keys
        ) AS balance_index,
        pre_balances [balance_index] :: INTEGER AS pre_staked_balance,
        post_balances [balance_index] :: INTEGER AS post_staked_balance,
        instruction :parsed :info :voteAccount :: STRING AS vote_acct
    FROM
        all_actions
),
validators AS (
    SELECT
        VALUE :nodePubkey :: STRING AS node_pubkey,
        VALUE :commission :: INTEGER AS commission,
        VALUE :votePubkey :: STRING AS vote_pubkey,
        RANK() over (
            ORDER BY
                VALUE :activatedStake :: INTEGER DESC
        ) AS validator_rank
    FROM
        {{ source(
            'solana_external',
            'validator_metadata_api'
        ) }}
),
remove_nulls AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        event_type,
        signers,
        stake_authority,
        CASE
            WHEN stake_acct IS NULL THEN LAST_VALUE(stake_acct) ignore nulls over (
                PARTITION BY signers [0] :: STRING
                ORDER BY
                    block_timestamp
            )
            ELSE stake_acct
        END AS stake_account,
        pre_staked_balance,
        post_staked_balance,
        CASE
            WHEN vote_acct IS NULL THEN LAST_VALUE(vote_acct) ignore nulls over (
                PARTITION BY signers [0] :: STRING
                ORDER BY
                    block_timestamp
            )
            ELSE vote_acct
        END AS vote_account
    FROM
        tx_base
),
balance_adjust_tx AS (
    SELECT
        tx_id
    FROM
        {{ ref('silver__staking_lp_actions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= CURRENT_DATE - 1
{% endif %}
GROUP BY
    tx_id
HAVING
    COUNT(tx_id) > 1
),
balance_adjust_index AS (
    SELECT
        A.tx_id,
        INDEX,
        event_type,
        pre_staked_balance,
        post_staked_balance
    FROM
        balance_adjust_tx A
        INNER JOIN tx_base b
        ON A.tx_id = b.tx_id
    WHERE
        event_type = 'split'
        OR event_type = 'initialize'
),
new_bal AS (
    SELECT
        b.tx_id,
        b.index,
        b.event_type,
        CASE
            WHEN b.index > ai.index THEN ai.post_staked_balance
            WHEN ai.event_type = 'initialize' THEN 0
            ELSE b.pre_staked_balance
        END AS pre_staked_balance,
        CASE
            WHEN b.event_type = 'deactivate' THEN 0
            WHEN ai.event_type = 'initialize' THEN 0
            ELSE b.post_staked_balance
        END AS post_staked_balance
    FROM
        tx_base b
        LEFT OUTER JOIN balance_adjust_index ai
        ON b.tx_id = ai.tx_id
    WHERE
        (
            ai.event_type = 'split'
            OR ai.event_type = 'initialize'
        )
        AND b.tx_id IN (
            SELECT
                tx_id
            FROM
                balance_adjust_tx
        )
)
SELECT
    block_id,
    block_timestamp,
    b.tx_id,
    succeeded,
    b.index,
    b.event_type,
    signers,
    stake_authority,
    stake_account,
    n.pre_staked_balance,
    n.post_staked_balance,
    vote_account,
    node_pubkey,
    validator_rank,
    commission,
    COALESCE(
        label,
        vote_account
    ) AS validator_name
FROM
    remove_nulls b
    LEFT OUTER JOIN new_bal n
    ON b.tx_id = n.tx_id
    AND b.index = n.index
    AND b.event_type = n.event_type
    LEFT OUTER JOIN validators v
    ON vote_account = vote_pubkey
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    ON vote_account = address
