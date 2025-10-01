{{ config(
    materialized = 'incremental',
    unique_key = ['token_daily_balances_id'],
    incremental_predicates = ["dynamic_range_predicate", "balance_date"],
    cluster_by = ['balance_date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(account, mint)'),
    tags = ['daily_balances']
) }}

WITH date_spine AS (
    SELECT
        date_day AS balance_date
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
    WHERE
        date_day < SYSDATE() :: DATE

{% if is_incremental() %}
AND date_day > (
    SELECT
        MAX(balance_date)
    FROM
        {{ this }}
)
-- Limit to next 60 days for backfill batching
AND date_day <= (
    SELECT
        LEAST(
            MAX(balance_date) + 60,
            CURRENT_DATE()
        )
    FROM
        {{ this }}
)
{% else %}
    AND date_day >= '2021-01-30'
    AND date_day <= '2021-04-01'-- First 2 months only
{% endif %}
),

{% if is_incremental() %}
latest_balances_from_table AS (
    SELECT
        account,
        mint,
        amount,
        owner,
        last_balance_change,
        balance_date
    FROM {{ this }}
    WHERE balance_date = (
        SELECT MAX(balance_date)
        FROM {{ this }}
    )
),
{% endif %}

todays_balance_changes AS (
    -- Get balance changes for dates in the date spine
    SELECT
        block_timestamp::DATE AS balance_date,
        account_address AS account,
        mint,
        balance AS amount,
        owner,
        block_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY block_timestamp::DATE, account_address, mint 
            ORDER BY block_timestamp DESC, block_id DESC, tx_index DESC
        ) AS daily_rank
    FROM {{ ref('core__fact_token_balances') }} tb
    WHERE EXISTS (
            SELECT 1 FROM date_spine ds 
            WHERE ds.balance_date = tb.block_timestamp::DATE
        )
),

todays_final_balances AS (
    -- Get the last balance change per account-mint for today
    SELECT
        balance_date,
        account,
        mint,
        amount,
        owner,
        block_timestamp AS last_balance_change_timestamp,
        TRUE AS balance_changed_on_date
    FROM todays_balance_changes
    WHERE daily_rank = 1
),

account_mint_combinations AS (
    -- Get all unique account-mint combinations that have ever had a balance
    SELECT DISTINCT
        account,
        mint
    FROM todays_final_balances
),

source_data AS (
    {% if is_incremental() %}
    -- Check if processing multiple days (batch mode)
    {% if execute %}
        {% set max_date_query %}
            SELECT MAX(balance_date) as max_date FROM {{ this }}
        {% endset %}
        {% set max_date = run_query(max_date_query).columns[0].values()[0] %}
        {% set days_to_process = (modules.datetime.date.today() - max_date).days %}
        {% set batch_size = days_to_process if days_to_process <= 60 else 60 %}
    {% else %}
        {% set batch_size = 1 %}
    {% endif %}
    
    {% if batch_size > 1 %}
    -- Multi-day batch: Use window functions for proper forward-filling
    SELECT
        d.balance_date,
        COALESCE(c.account, y.account) AS account,
        COALESCE(c.mint, y.mint) AS mint,
        -- For amount, use the most recent change within batch, or carry forward from yesterday
        COALESCE(
            LAST_VALUE(t.amount IGNORE NULLS) OVER (
                PARTITION BY COALESCE(c.account, y.account), COALESCE(c.mint, y.mint)
                ORDER BY d.balance_date 
                ROWS UNBOUNDED PRECEDING
            ),
            y.amount
        ) AS amount,
        -- For owner, use the most recent change within batch, or carry forward from yesterday  
        COALESCE(
            LAST_VALUE(t.owner IGNORE NULLS) OVER (
                PARTITION BY COALESCE(c.account, y.account), COALESCE(c.mint, y.mint)
                ORDER BY d.balance_date 
                ROWS UNBOUNDED PRECEDING
            ),
            y.owner
        ) AS owner,
        -- For last_balance_change, we need to track the most recent change date within the batch
        CASE 
            WHEN MAX(CASE WHEN t.balance_date IS NOT NULL THEN d.balance_date END) OVER (
                PARTITION BY COALESCE(c.account, y.account), COALESCE(c.mint, y.mint)
                ORDER BY d.balance_date 
                ROWS UNBOUNDED PRECEDING
            ) IS NOT NULL THEN 
                MAX(CASE WHEN t.balance_date IS NOT NULL THEN d.balance_date END) OVER (
                    PARTITION BY COALESCE(c.account, y.account), COALESCE(c.mint, y.mint)
                    ORDER BY d.balance_date 
                    ROWS UNBOUNDED PRECEDING
                )::TIMESTAMP
            ELSE y.last_balance_change::TIMESTAMP
        END AS last_balance_change_timestamp,
        CASE WHEN t.balance_date IS NOT NULL THEN TRUE ELSE FALSE END AS balance_changed_on_date
    FROM date_spine d
    CROSS JOIN (
        -- All accounts that should exist (previous + new)
        SELECT account, mint FROM latest_balances_from_table
        UNION 
        SELECT account, mint FROM account_mint_combinations
    ) c
    LEFT JOIN todays_final_balances t 
        ON d.balance_date = t.balance_date
        AND c.account = t.account 
        AND c.mint = t.mint
    LEFT JOIN latest_balances_from_table y 
        ON c.account = y.account 
        AND c.mint = y.mint
    
    {% else %}
    -- Single day: Use original efficient logic
    SELECT 
        balance_date,
        account,
        mint,
        amount,
        owner,
        last_balance_change_timestamp,
        balance_changed_on_date
    FROM todays_final_balances
    
    UNION ALL
    
    -- Carry forward yesterday's balances for accounts that didn't change today
    SELECT 
        d.balance_date,
        y.account,
        y.mint,
        y.amount,
        y.owner,
        y.last_balance_change::TIMESTAMP AS last_balance_change_timestamp,
        FALSE AS balance_changed_on_date
    FROM date_spine d
    CROSS JOIN latest_balances_from_table y
    LEFT JOIN todays_final_balances t 
        ON y.account = t.account 
        AND y.mint = t.mint
        AND d.balance_date = t.balance_date
    WHERE t.account IS NULL  -- Only accounts with no changes today
    {% endif %}
    
    {% else %}
    -- Full refresh: Create complete time series with forward-filling
    SELECT
        d.balance_date,
        c.account,
        c.mint,
        LAST_VALUE(t.amount IGNORE NULLS) OVER (
            PARTITION BY c.account, c.mint 
            ORDER BY d.balance_date 
            ROWS UNBOUNDED PRECEDING
        ) AS amount,
        LAST_VALUE(t.owner IGNORE NULLS) OVER (
            PARTITION BY c.account, c.mint 
            ORDER BY d.balance_date 
            ROWS UNBOUNDED PRECEDING
        ) AS owner,
        LAST_VALUE(t.last_balance_change_timestamp IGNORE NULLS) OVER (
            PARTITION BY c.account, c.mint 
            ORDER BY d.balance_date 
            ROWS UNBOUNDED PRECEDING
        ) AS last_balance_change_timestamp,
        CASE WHEN t.balance_date IS NOT NULL THEN TRUE ELSE FALSE END AS balance_changed_on_date
    FROM date_spine d
    CROSS JOIN account_mint_combinations c
    LEFT JOIN todays_final_balances t 
        ON d.balance_date = t.balance_date
        AND c.account = t.account 
        AND c.mint = t.mint
    {% endif %}
)

SELECT
    balance_date,
    account,
    mint,
    amount,
    owner,
    last_balance_change_timestamp::DATE AS last_balance_change,
    balance_changed_on_date,
    {{ dbt_utils.generate_surrogate_key(['balance_date', 'account', 'mint']) }} AS token_daily_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM source_data
WHERE amount IS NOT NULL  -- Only include accounts that have had at least one balance
    AND amount > 0  -- Only include accounts with positive balances
