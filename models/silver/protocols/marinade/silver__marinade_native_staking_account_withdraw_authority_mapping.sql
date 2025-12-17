{{
    config(
        materialized = 'table',
        tags = ['scheduled_non_core'],
    )
}}

/*
Remap withdraw authority using improved recursion to find root parent.
For Marinade native staking accounts (stake_authority = stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq),
traverse parent relationships to find the root parent's withdraw_authority.
*/
WITH marinade_native_stakers AS (
    SELECT DISTINCT 
        stake_account
    FROM 
        {{ ref('silver__staking_lp_actions_labeled_2') }}
    WHERE
        stake_authority IN ('stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq', 'ex9CfkBZZd6Nv9XdnoDmmB45ymbu4arXVk7g5pWnt3N','STNi1NHDUi6Hvibvonawgze8fM83PFLeJhuGMEXyGps')
),

base_accounts AS (
    -- Pre-filter to get latest record for each stake account before recursion
    -- This avoids window functions in the recursive CTE
    SELECT
        stake_account,
        parent_stake_account,
        block_timestamp,
        event_type,
        withdraw_authority,
        stake_authority
    FROM {{ ref('silver__staking_lp_actions_labeled_2') }}
    WHERE event_type IN ('split_destination', 'initialize','initializeChecked','authorize')
),
filtered_base AS (
    -- Get only the first/oldest record for each stake account (when it was created)
    -- Include ALL accounts (even with Marinade withdraw_authority) to allow full parent traversal
    SELECT
        stake_account,
        parent_stake_account,
        block_timestamp,
        event_type,
        withdraw_authority,
        stake_authority
    FROM base_accounts
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY stake_account
            /* This logic finds the real withdraw auth from the parent*/
            ORDER BY  IFF(stake_authority = 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq' and withdraw_authority <> 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq',0,1)
            ,IFF(withdraw_authority in ('opNS8ENpEMWdXcJUgJCsJTDp7arTXayoBEeBUg6UezP','noMa7dN4cHQLV4ZonXrC29HTKFpxrpFbDLK5Gub8W8t','MMMar7NHxj2G427CR6qMev9SEJAq36HhhwUFo4fCd5k'),1,0)
            ,IFF(parent_stake_account is not null,0,1)
            ,IFF(stake_authority = 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq',-(EXTRACT(EPOCH FROM block_timestamp)),EXTRACT(EPOCH FROM block_timestamp)) 
            ,block_timestamp asc
        ) = 1
),
stake_lineage AS (
    -- Base case: Start with all accounts but limit fields to reduce memory
    SELECT
        f.stake_account,
        f.parent_stake_account,
        f.withdraw_authority,
        0 AS depth,
        f.stake_account AS original_account,
        f.stake_authority
    FROM filtered_base f

    UNION ALL

    -- Recursive case: Traverse up to parent accounts with memory optimizations
    SELECT
        f.stake_account,
        f.parent_stake_account,
        f.withdraw_authority,
        l.depth + 1 AS depth,
        l.original_account,
        l.stake_authority
    FROM stake_lineage l
    INNER JOIN filtered_base f
        ON l.parent_stake_account = f.stake_account
    WHERE l.parent_stake_account IS NOT NULL
        AND l.depth < 150  -- Reasonable recursion limit for memory efficiency
),
root_parents AS (
    -- For each original account, find its deepest parent
    -- This is either the true root (no parent) or the deepest parent found within recursion limit
    SELECT
        original_account AS stake_account,
        stake_account AS root_parent_account,
        withdraw_authority AS root_withdraw_authority,
        depth AS chain_depth
    FROM stake_lineage
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY original_account
        ORDER BY depth DESC
    ) = 1
),
accounts_with_mapped_authority AS (
    SELECT
        stake_account,
        root_withdraw_authority AS withdraw_authority,
        chain_depth
    FROM root_parents
    WHERE root_withdraw_authority IS NOT NULL
    and stake_account IN (SELECT stake_account FROM marinade_native_stakers)
)
SELECT
    stake_account,
    withdraw_authority,
    {{ dbt_utils.generate_surrogate_key(['stake_account']) }} AS marinade_native_staking_account_withdraw_authority_mapping_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM accounts_with_mapped_authority
