-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['stablecoins_daily_supply_by_mint_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_date','modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}


WITH verified_stablecoins AS (

    SELECT
        token_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        -- is_verified
        -- AND 
        token_address IS NOT NULL
)

    SELECT
        balance_date as block_date,
        mint,
        sum(amount) as supply,
        {{ dbt_utils.generate_surrogate_key(['block_date','mint']) }} AS stablecoins_daily_supply_by_mint_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM {{ ref('core__fact_token_daily_balances') }} a
        INNER JOIN verified_stablecoins b on a.mint = b.token_address
    where balance_date = '2025-10-29'
    -- {% if is_incremental() %}
    -- where balance_date >= (
    --     SELECT
    --         MAX(balance_date)
    --     FROM
    --         {{ this }}
    -- )
    -- {% endif %}
    group by 1,2


