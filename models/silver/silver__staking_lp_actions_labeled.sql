{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','tx_id','index'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH base_staking_lp_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__staking_lp_actions') }}
    {% if is_incremental() %}
    WHERE block_timestamp::date >= current_date - 1
    {% endif %}
),
{% if is_incremental() %}
latest_state as (
    select stake_account, stake_authority, withdraw_authority, stake_active, vote_account
    from {{ this }}
    WHERE block_timestamp::date < current_date - 1
    qualify(row_number() over (partition by stake_account order by block_id desc, block_timestamp desc, index desc)) = 1
),
{% else %}
/*dummy cte so that downstream code can stay the same*/
latest_state as (
    select 
        'a' as stake_account, 
        'a' as stake_authority, 
        'a' as withdraw_authority, 
        'a' as stake_active, 
        'a' as vote_account
),
{% endif %}
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
        instruction,
        _inserted_timestamp
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
        instruction,
        _inserted_timestamp
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
        instruction,
        _inserted_timestamp
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
        instruction,
        _inserted_timestamp
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
        instruction,
        _inserted_timestamp
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
            WHEN event_type = 'split_destination' THEN stake_authority
            ELSE NULL
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
        END AS withdraw_destination,
        _inserted_timestamp
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
        {{ ref(
            'silver__validator_metadata_api'
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
        COALESCE(tx_base.stake_authority, 
            LAST_VALUE(tx_base.stake_authority) ignore nulls over (
                PARTITION BY tx_base.stake_account
                ORDER BY
                    block_id,
                    INDEX rows unbounded preceding
            ),
            latest_state.stake_authority) AS stake_authority,
        COALESCE(tx_base.withdraw_authority, 
            LAST_VALUE(tx_base.withdraw_authority) ignore nulls over (
                PARTITION BY tx_base.stake_account
                ORDER BY
                    block_id,
                    INDEX rows unbounded preceding
            ),
            latest_state.withdraw_authority) AS withdraw_authority,
        tx_base.stake_account,
        COALESCE(tx_base.stake_active, 
            LAST_VALUE(tx_base.stake_active) ignore nulls over (
                PARTITION BY tx_base.stake_account
                ORDER BY
                    block_id,
                    INDEX rows unbounded preceding
            ),
            latest_state.stake_active) AS stake_active,
        pre_tx_staked_balance,
        post_tx_staked_balance,
        withdraw_amount,
        withdraw_destination,
        COALESCE(tx_base.vote_acct,
            LAST_VALUE(tx_base.vote_acct) ignore nulls over (
                PARTITION BY tx_base.stake_account
                ORDER BY
                    block_id,
                    INDEX rows unbounded preceding
            ),
            latest_state.vote_account) AS vote_account,
        tx_base._inserted_timestamp
    FROM
        tx_base
    LEFT OUTER JOIN latest_state on latest_state.stake_account = tx_base.stake_account
),
temp AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        b.tx_id,
        b.succeeded,
        b.index,
        b.event_type,
        b.signers,
        b.stake_authority,
        b.withdraw_authority,
        b.stake_account,
        b.stake_active,
        b.pre_tx_staked_balance,
        b.post_tx_staked_balance,
        b.withdraw_amount,
        b.withdraw_destination,
        COALESCE(
            b.vote_account,
            A.vote_account
        ) AS vote_account,
        b._inserted_timestamp
    FROM
        fill_vote_acct b
        LEFT OUTER JOIN fill_vote_acct A
        ON b.tx_id = A.tx_id
        AND b.index = A.index
        AND b.event_type = 'split_destination'
        AND A.event_type = 'split_source'
),
temp2 AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        INDEX,
        event_type,
        signers,
        stake_authority,
        withdraw_authority,
        stake_account,
        stake_active,
        pre_tx_staked_balance,
        post_tx_staked_balance,
        withdraw_amount,
        withdraw_destination,
        CASE
            WHEN vote_account IS NULL THEN LAST_VALUE(vote_account) ignore nulls over (
                PARTITION BY stake_account
                ORDER BY
                    block_id,
                    INDEX rows unbounded preceding
            )
            ELSE vote_account
        END AS vote_account,
        _inserted_timestamp
    FROM
        temp
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    INDEX,
    event_type,
    signers,
    stake_authority,
    withdraw_authority,
    stake_account,
    stake_active,
    pre_tx_staked_balance,
    post_tx_staked_balance,
    withdraw_amount,
    withdraw_destination,
    vote_account,
    node_pubkey,
    validator_rank,
    commission,
    COALESCE(
        address_name,
        vote_account
    ) AS validator_name,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','tx_id','index']
    ) }} AS staking_lp_actions_labeled_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    temp2
    LEFT OUTER JOIN validators v
    ON vote_account = vote_pubkey
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    ON vote_account = address
