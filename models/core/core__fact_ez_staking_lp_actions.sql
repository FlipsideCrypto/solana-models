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

UNION

    SELECT
        * 
    FROM 
        {{ ref('silver___historical_staking_lp_actions') }}

    WHERE block_id <= 109547725
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
        LEAD(
            event_type,
            1
        ) over (
            PARTITION BY stake_account
            ORDER BY
                block_id,
                INDEX
        ) AS next_event_type,
        signers,
        CASE
            WHEN event_type = 'initialize' THEN instruction :parsed :info :authorized :staker :: STRING
            WHEN event_type = 'authorize'
            AND instruction :parsed :info :authorityType = 'Staker' THEN instruction :parsed :info :newAuthority :: STRING
            ELSE instruction :parsed :info :stakeAuthority :: STRING
        END AS stake_authority,
        CASE
            WHEN event_type = 'initialize' THEN instruction :parsed :info :authorized :withdrawer :: STRING
            WHEN event_type = 'authorize'
            AND instruction :parsed :info :authorityType = 'Withdrawer' THEN instruction :parsed :info :newAuthority :: STRING
            ELSE instruction :parsed :info :stakeAuthority :: STRING
        END AS withdraw_authority,
        stake_account,
        CASE
            WHEN event_type = 'delegate' THEN TRUE
            WHEN next_event_type = 'delegate' THEN FALSE
            WHEN next_event_type = 'deactivate' THEN TRUE
            WHEN event_type IN (
                'deactivate',
                'merge_source'
            ) THEN FALSE
            ELSE NULL
        END AS stake_active,
        silver.udf_get_account_balances_index(
            stake_account,
            account_keys
        ) AS balance_index,
        pre_balances [balance_index] :: INTEGER AS pre_tx_staked_balance,
        post_balances [balance_index] :: INTEGER AS post_tx_staked_balance,
        instruction :parsed :info :voteAccount :: STRING AS vote_acct,
        CASE
            WHEN event_type = 'withdraw' THEN instruction :parsed :info :lamports :: NUMBER
            ELSE NULL
        END AS withdraw_amount,
        CASE
            WHEN event_type = 'withdraw' THEN instruction :parsed :info :destination :: STRING
            ELSE NULL
        END AS withdraw_destination
    FROM
        all_actions
),
validators AS (
    SELECT
        VALUE :nodePubkey :: STRING AS node_pubkey,
        VALUE :commission :: INTEGER AS commission,
        VALUE :votePubkey :: STRING AS vote_pubkey,
        VALUE :number :: INTEGER AS validator_rank
    FROM
        {{ source(
            'solana_external',
            'validator_metadata_api'
        ) }}
),
fill_vote_acct AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        event_type,
        signers,
        CASE
            WHEN stake_authority IS NULL THEN LAST_VALUE(stake_authority) ignore nulls over (
                PARTITION BY stake_authority
                ORDER BY
                    block_id,
                    INDEX rows unbounded preceding
            )
            ELSE stake_authority
        END AS stake_authority,
        CASE
            WHEN withdraw_authority IS NULL THEN LAST_VALUE(withdraw_authority) ignore nulls over (
                PARTITION BY withdraw_authority
                ORDER BY
                    block_id,
                    INDEX rows unbounded preceding
            )
            ELSE withdraw_authority
        END AS withdraw_authority,
        stake_account,
        CASE
            WHEN stake_active IS NULL THEN LAST_VALUE(stake_active) ignore nulls over (
                PARTITION BY stake_account
                ORDER BY
                    block_id,
                    INDEX rows unbounded preceding
            )
            ELSE stake_active
        END AS stake_active,
        pre_tx_staked_balance,
        post_tx_staked_balance,
        withdraw_amount,
        withdraw_destination,
        CASE
            WHEN vote_acct IS NULL THEN LAST_VALUE(vote_acct) ignore nulls over (
                PARTITION BY stake_account
                ORDER BY
                    block_id,
                    INDEX rows unbounded preceding
            )
            ELSE vote_acct
        END AS vote_account
    FROM
        tx_base
), 
vote_acct_splits AS (
    SELECT 
        tx_id, 
        index, 
        vote_account
    FROM fill_vote_acct 
    WHERE 
        vote_account IS NOT NULL 
    AND (event_type = 'split_source' 
    OR event_type = 'split_destination')
), 
vote_acct_joins AS (
    SELECT
        v.tx_id, 
        v.index, 
        a.event_type, 
        v.vote_account
    FROM {{ ref('silver__staking_lp_actions') }} a

    INNER JOIN vote_acct_splits v
    ON v.tx_id = a.tx_id
    AND v.index = a.index
) 
SELECT
    b.block_id,
    b.block_timestamp,
    b.tx_id,
    succeeded,
    b.index,
    b.event_type,
    signers,
    stake_authority,
    withdraw_authority,
    stake_account,
    stake_active,
    pre_tx_staked_balance,
    post_tx_staked_balance,
    withdraw_amount,
    withdraw_destination,
    COALESCE(
        b.vote_account,
        va.vote_account
    ) AS vote_account,  
    node_pubkey,
    validator_rank,
    commission,
    COALESCE(
        label,
        b.vote_account, 
        va.vote_account
    ) AS validator_name
FROM
    fill_vote_acct b 
    LEFT OUTER JOIN vote_acct_joins va
    ON b.tx_id = va.tx_id
    AND b.index = va.index
    LEFT OUTER JOIN validators v
    ON (b.vote_account = vote_pubkey
    OR va.vote_account = vote_pubkey)
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    ON b.vote_account = address
   
