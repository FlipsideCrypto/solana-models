{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_id','tx_id','index'],
    cluster_by = ['block_timestamp::DATE','event_type'],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id,stake_authority,withdraw_authority,stake_account)'),
    tags = ['scheduled_non_core']
) }}

WITH base_staking_lp_actions AS (
    SELECT
        *
    FROM
        {{ ref('silver__staking_lp_actions_2') }}
    {% if is_incremental() %}
    WHERE 
        block_timestamp::date >= current_date - 1
    {% endif %}
    and succeeded
),

{% if is_incremental() %}
latest_state AS (
    SELECT 
        stake_account,
        stake_authority,
        withdraw_authority,
        stake_active,
        vote_account
    FROM 
        {{ this }}
    WHERE 
        block_timestamp::date < current_date - 1
    QUALIFY
        row_number() OVER (
            PARTITION BY stake_account 
            ORDER BY block_id DESC, block_timestamp DESC, index DESC, inner_index DESC
        ) = 1
),
{% else %}
/*dummy cte so that downstream code can stay the same*/
latest_state AS (
    SELECT 
        'a' AS stake_account,
        'a' AS stake_authority,
        'a' AS withdraw_authority,
        'a' AS stake_active,
        'a' AS vote_account
),
{% endif %}
merges_and_splits AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        signers,
        instruction:parsed:info:stakeAccount::string AS stake_account,
        NULL AS parent_stake_account,
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
    
    UNION ALL
    
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        signers,
        instruction:parsed:info:newSplitAccount::string AS stake_account,
        instruction:parsed:info:stakeAccount::string AS parent_stake_account,
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
    
    UNION ALL
    
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        signers,
        instruction:parsed:info:destination::string AS stake_account,
        NULL AS parent_stake_account,
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
    
    UNION ALL
    
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        signers,
        instruction:parsed:info:source::string AS stake_account,
        NULL AS parent_stake_account,
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

    UNION ALL

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        signers,
        instruction:parsed:info:source::string AS stake_account,
        NULL AS parent_stake_account,
        event_type,
        account_keys,
        pre_balances,
        post_balances,
        instruction,
        _inserted_timestamp
    FROM
        base_staking_lp_actions
    WHERE
        event_type in ('moveLamports','moveStake')
),

all_actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        signers,
        instruction:parsed:info:stakeAccount::string AS stake_account,
        NULL AS parent_stake_account,
        event_type,
        account_keys,
        pre_balances,
        post_balances,
        instruction,
        _inserted_timestamp
    FROM
        base_staking_lp_actions
    WHERE
        event_type NOT IN ('merge', 'split',  'moveLamports','moveStake')
    
    UNION ALL
    
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
        index,
        inner_index,
        event_type,
        lead(event_type, 1) OVER (
            PARTITION BY stake_account
            ORDER BY block_id, index, inner_index
        ) AS next_event_type,
        signers,
        CASE
            WHEN event_type = 'initialize' THEN instruction:parsed:info:authorized:staker::string
            WHEN event_type = 'initializeChecked' THEN instruction:parsed:info:staker::string
            WHEN event_type in ('authorize','authorizeChecked') AND instruction:parsed:info:authorityType = 'Staker' THEN instruction:parsed:info:newAuthority::string
            ELSE instruction:parsed:info:stakeAuthority::string
        END AS stake_authority,
        CASE
            WHEN event_type = 'initialize' THEN instruction:parsed:info:authorized:withdrawer::string
            WHEN event_type = 'initializeChecked' THEN instruction:parsed:info:withdrawer::string
            WHEN event_type in ('authorize','authorizeChecked') AND instruction:parsed:info:authorityType = 'Withdrawer' THEN instruction:parsed:info:newAuthority::string
            ELSE NULL
        END AS withdraw_authority,
        stake_account,
        parent_stake_account,
        CASE
            WHEN event_type in ('delegate', 'moveStake', 'moveLamports') THEN TRUE
            WHEN next_event_type = 'delegate' THEN FALSE
            WHEN next_event_type = 'deactivate' THEN TRUE
            WHEN event_type IN ('deactivate', 'merge_source') THEN FALSE
            ELSE NULL
        END AS stake_active,
        silver.udf_get_account_balances_index(stake_account, account_keys) AS balance_index,
        pre_balances[balance_index]::integer AS pre_tx_staked_balance,
        post_balances[balance_index]::integer AS post_tx_staked_balance,
        instruction:parsed:info:voteAccount::string AS vote_acct,
        CASE
            WHEN event_type = 'withdraw' THEN instruction:parsed:info:lamports::number
            ELSE NULL
        END AS withdraw_amount,
        CASE
            WHEN event_type = 'withdraw' THEN instruction:parsed:info:destination::string
            ELSE NULL
        END AS withdraw_destination,
        CASE
            WHEN event_type in ('moveStake','moveLamports') THEN instruction:parsed:info:lamports::number
            ELSE NULL
        END AS move_amount,
        CASE
            WHEN event_type in ('moveStake','moveLamports') THEN instruction:parsed:info:destination::string
            ELSE NULL
        END AS move_destination,
        _inserted_timestamp
    FROM
        all_actions
),

validators AS (
    SELECT
        r.value:identity::string AS node_pubkey,
        r.value:commission::integer AS commission,
        r.value:vote_identity::string AS vote_pubkey,
        r.value:activated_stake::float AS stake,
        row_number() OVER (ORDER BY stake DESC) AS validator_rank
    FROM
        {{ ref('bronze__streamline_validator_metadata_2') }} AS v,
        table(flatten(data::array)) AS r
    QUALIFY
        row_number() OVER(ORDER BY v._partition_by_created_date DESC, v._inserted_timestamp DESC) = 1
),

fill_vote_acct AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        event_type,
        signers,
        coalesce(
            tx_base.stake_authority,
            last_value(tx_base.stake_authority) IGNORE NULLS OVER (
                PARTITION BY tx_base.stake_account
                ORDER BY block_id, index, inner_index 
                ROWS UNBOUNDED PRECEDING
            ),
            latest_state.stake_authority
        ) AS stake_authority,
        coalesce(
            tx_base.withdraw_authority,
            last_value(tx_base.withdraw_authority) IGNORE NULLS OVER (
                PARTITION BY tx_base.stake_account
                ORDER BY block_id, index, COALESCE(inner_index, -1)
                ROWS UNBOUNDED PRECEDING
            ),
            latest_state.withdraw_authority
        ) AS withdraw_authority,
        tx_base.stake_account,
        tx_base.parent_stake_account,
        coalesce(
            tx_base.stake_active,
            last_value(tx_base.stake_active) IGNORE NULLS OVER (
                PARTITION BY tx_base.stake_account
                ORDER BY block_id, index, inner_index 
                ROWS UNBOUNDED PRECEDING
            ),
            latest_state.stake_active
        ) AS stake_active,
        pre_tx_staked_balance,
        post_tx_staked_balance,
        withdraw_amount,
        withdraw_destination,
        move_amount,
        move_destination,
        coalesce(
            tx_base.vote_acct,
            last_value(tx_base.vote_acct) IGNORE NULLS OVER (
                PARTITION BY tx_base.stake_account
                ORDER BY block_id, index, inner_index 
                ROWS UNBOUNDED PRECEDING
            ),
            latest_state.vote_account
        ) AS vote_account,
        tx_base._inserted_timestamp
    FROM
        tx_base
    LEFT OUTER JOIN 
        latest_state 
        ON latest_state.stake_account = tx_base.stake_account
),


temp AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        b.tx_id,
        b.succeeded,
        b.index,
        b.inner_index,
        b.event_type,
        b.signers,
        -- For split_destination, inherit stake_authority from split_source if available
        CASE 
            WHEN b.event_type = 'split_destination' AND A.stake_authority IS NOT NULL 
            THEN A.stake_authority
            ELSE b.stake_authority
        END AS stake_authority,
        b.withdraw_authority,
        b.stake_account,
        b.parent_stake_account,
        b.stake_active,
        b.pre_tx_staked_balance,
        b.post_tx_staked_balance,
        b.withdraw_amount,
        b.withdraw_destination,
        b.move_amount,
        b.move_destination,
        coalesce(b.vote_account, A.vote_account) AS vote_account,
        b._inserted_timestamp
    FROM
        fill_vote_acct b
    LEFT OUTER JOIN 
        fill_vote_acct A
        ON b.tx_id = A.tx_id
        AND b.index = A.index
        AND coalesce(b.inner_index, -1) = coalesce(A.inner_index, -1)
        AND b.event_type = 'split_destination'
        AND A.event_type = 'split_source'
),

-- Step 1: Add parent withdraw_authority for split inheritance using window functions
temp_with_parent_authority AS (
    SELECT 
        *,
        -- For split_destination events, get the withdraw_authority from the parent account
        -- Use a window function to find the most recent withdraw_authority for the parent
        CASE 
            WHEN event_type = 'split_destination' AND parent_stake_account IS NOT NULL THEN
                LAST_VALUE(
                    CASE WHEN stake_account = parent_stake_account THEN withdraw_authority END
                ) IGNORE NULLS OVER (
                    PARTITION BY tx_id
                    ORDER BY index, COALESCE(inner_index, -1)
                    ROWS UNBOUNDED PRECEDING
                )
            ELSE NULL
        END AS parent_withdraw_authority
    FROM temp
),

-- Step 2: Apply inheritance logic
temp_with_inheritance AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        event_type,
        signers,
        stake_authority,
        -- Apply the three inheritance rules
        COALESCE(
            -- 1. Use explicit withdraw_authority if set by the event
            withdraw_authority,
            -- 2. For split_destination, inherit from parent
            parent_withdraw_authority,
            -- 3. Use temporal inheritance within same stake_account
            LAST_VALUE(withdraw_authority) IGNORE NULLS OVER (
                PARTITION BY stake_account
                ORDER BY block_id, index, COALESCE(inner_index, -1)
                ROWS UNBOUNDED PRECEDING
            ),
            -- 4. Fallback for completely new accounts
            signers[0]::string
        ) AS withdraw_authority,
        stake_account,
        parent_stake_account,
        stake_active,
        pre_tx_staked_balance,
        post_tx_staked_balance,
        withdraw_amount,
        withdraw_destination,
        move_amount,
        move_destination,
        vote_account,
        _inserted_timestamp
    FROM temp_with_parent_authority
),

temp2 AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        inner_index,
        event_type,
        signers,
        stake_authority,
        withdraw_authority,
        stake_account,
        parent_stake_account,
        stake_active,
        pre_tx_staked_balance,
        post_tx_staked_balance,
        withdraw_amount,
        withdraw_destination,
        move_amount,
        move_destination,
        CASE
            WHEN vote_account IS NULL 
            THEN last_value(vote_account) IGNORE NULLS OVER (
                PARTITION BY stake_account
                ORDER BY block_id, index, inner_index 
                ROWS UNBOUNDED PRECEDING
            )
            ELSE vote_account
        END AS vote_account,
        _inserted_timestamp
    FROM
        temp_with_inheritance
)

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    signers,
    stake_authority,
    withdraw_authority,
    stake_account,
    parent_stake_account,
    stake_active,
    pre_tx_staked_balance,
    post_tx_staked_balance,
    withdraw_amount,
    withdraw_destination,
    move_amount,
    move_destination,
    vote_account,
    node_pubkey,
    validator_rank,
    commission,
    coalesce(address_name, vote_account) AS validator_name,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'index', 'inner_index', 'event_type']
    ) }} AS staking_lp_actions_labeled_2_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    temp2
LEFT OUTER JOIN 
    validators v
    ON vote_account = vote_pubkey
LEFT OUTER JOIN 
    {{ ref('core__dim_labels') }}
    ON vote_account = address
