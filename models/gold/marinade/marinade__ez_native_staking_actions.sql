{{
    config(
        materialized = 'table',
        cluster_by = ['block_timestamp::DATE'],
        post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id,stake_account,provider_address)'),
    )
}}

WITH marinade_native_stakers AS (
    SELECT DISTINCT 
        stake_account
    FROM 
        {{ ref('silver__staking_lp_actions_labeled_2') }}
    WHERE
        stake_authority = 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq'
),
withdraw_authority_from_snapshot_tmp AS (
    SELECT 
        m.stake_account,
        w.authorized_withdrawer AS withdraw_authority,
        w.epoch,
        e.end_block AS end_block_id
    FROM 
        marinade_native_stakers AS m
    JOIN
        {{ ref('gov__fact_stake_accounts') }} AS w
        ON m.stake_account = w.stake_pubkey
    LEFT JOIN
        {{ ref('silver__epoch') }} AS e
        ON w.epoch = e.epoch
),
withdraw_authority_from_snapshot AS (
    SELECT 
        m.stake_account,
        m.withdraw_authority,
        e.start_block AS start_block_id,
        m.end_block_id
    FROM 
        withdraw_authority_from_snapshot_tmp AS m
    LEFT JOIN
        {{ ref('silver__epoch') }} AS e
        ON m.epoch-1 = e.epoch
),
reconcile_withdraw_authority_with_snapshot AS (
    SELECT 
        s.* exclude(withdraw_authority),
        CASE
            WHEN s.withdraw_authority = 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq' 
            THEN w.withdraw_authority
            ELSE s.withdraw_authority
        END AS withdraw_authority
    FROM 
        marinade_native_stakers AS m
    JOIN 
        {{ ref('silver__staking_lp_actions_labeled_2') }} AS s
        ON m.stake_account = s.stake_account
    LEFT JOIN
        withdraw_authority_from_snapshot AS w
        ON s.stake_account = w.stake_account
        AND s.block_id BETWEEN w.start_block_id AND w.end_block_id
),
reconcile_with_mapping AS (
    SELECT 
        s.* exclude(withdraw_authority),
        CASE
            WHEN s.withdraw_authority IS NULL 
            THEN w.withdraw_authority
            ELSE s.withdraw_authority
        END AS withdraw_authority
    FROM 
        reconcile_withdraw_authority_with_snapshot AS s
    LEFT JOIN
        {{ ref('silver__marinade_native_staking_account_withdraw_authority_mapping') }} AS w
        ON s.stake_account = w.stake_account
),
get_withdraw_authority_by_parent AS (
    SELECT DISTINCT
        s.stake_account,
        w.withdraw_authority
    FROM 
        reconcile_with_mapping AS s
    LEFT JOIN
        withdraw_authority_from_snapshot AS w
        ON s.parent_stake_account = w.stake_account
    WHERE 
        s.withdraw_authority IS NULL
        AND w.withdraw_authority IS NOT NULL
),
reconcile_with_parent AS (
    SELECT 
        s.* exclude(withdraw_authority),
        coalesce(s.withdraw_authority, w.withdraw_authority) AS withdraw_authority
    FROM 
        reconcile_with_mapping AS s
    LEFT JOIN
        get_withdraw_authority_by_parent AS w 
        ON s.stake_account = w.stake_account
        AND s.withdraw_authority IS NULL
),
get_withdraw_authority_with_window AS (
    SELECT 
        *,
        last_value(withdraw_authority IGNORE NULLS) OVER (
            PARTITION BY stake_account 
            ORDER BY block_timestamp, index 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS withdraw_authority_last_value,
        first_value(withdraw_authority IGNORE NULLS) OVER (
            PARTITION BY stake_account 
            ORDER BY block_timestamp, index 
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS withdraw_authority_first_value
    FROM 
        reconcile_with_parent
),
reconcile_with_window AS (
    SELECT 
        * exclude(withdraw_authority, withdraw_authority_last_value, withdraw_authority_first_value),
        coalesce(
            withdraw_authority, 
            withdraw_authority_last_value, 
            withdraw_authority_first_value
        ) AS withdraw_authority
    FROM 
        get_withdraw_authority_with_window
)
SELECT 
    block_id,
    block_timestamp,
    tx_id,
    index,
    inner_index,
    event_type AS action_type,
    withdraw_authority AS provider_address,
    stake_account,
    stake_active,
    stake_authority,
    pre_tx_staked_balance,
    post_tx_staked_balance,
    withdraw_destination,
    validator_name,
    vote_account,
    node_pubkey,
    validator_rank,
    'Stake11111111111111111111111111111111111111' AS program_id,
    'marinade native proxy' AS platform,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index']) }} AS marinade_native_ez_staking_actions_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    reconcile_with_window
