-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['marinade_liquid_staking_actions_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.marinade_liquid_staking_actions__intermediate_tmp AS
    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD'
        AND event_type IN ('deposit', 'depositStakeAccount', 'orderUnstake', 'claim')
        AND succeeded

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '1 hour'
        FROM
            {{ this }}
    )
    {% else %}
        AND _inserted_timestamp::DATE >= '2023-11-15'
    {% endif %}
    {% endset %}
    
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate(
        "silver.marinade_liquid_staking_actions__intermediate_tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

WITH base AS (
    SELECT
        *,
        COALESCE(
            LEAD(inner_index) OVER (
                PARTITION BY tx_id, index 
                ORDER BY inner_index
            ), 9999
        ) AS next_liquid_staking_action_inner_index
    FROM
        silver.marinade_liquid_staking_actions__intermediate_tmp
),
mints AS (
    SELECT 
        a.*
    FROM {{ ref('silver__token_mint_actions') }} a
        INNER JOIN (
            SELECT DISTINCT tx_id, block_timestamp::date as bt FROM base WHERE event_type IN ('deposit', 'depositStakeAccount')) b 
        ON b.tx_id = a.tx_id 
        AND b.bt = a.block_timestamp::date
    WHERE
        a.succeeded
        AND {{ between_stmts }}
),
transfers AS (
    SELECT
        a.* exclude(index),
        split_part(a.index,'.',1)::int AS index,
        nullif(split_part(a.index,'.',2),'')::int AS inner_index
    FROM 
        {{ ref('silver__transfers') }} a
        INNER JOIN (SELECT DISTINCT tx_id, block_timestamp::date as bt FROM base WHERE event_type = 'claim') b 
        ON b.tx_id = a.tx_id and b.bt = a.block_timestamp::date
    WHERE
        a.succeeded
        AND {{ between_stmts }}
),
sol_balances as (
    SELECT
        a.*
    FROM 
        {{ ref('silver__sol_balances') }} a
        INNER JOIN (SELECT DISTINCT tx_id, block_timestamp::date as bt FROM base WHERE event_type = 'depositStakeAccount') b 
        ON b.tx_id = a.tx_id and b.bt = a.block_timestamp::date
    WHERE
        a.succeeded
        AND {{ between_stmts }}
),
deposits AS (
    SELECT
        a.block_id,
        a.block_timestamp,
        a.tx_id,
        a.index,
        a.inner_index,
        a.event_type AS action_type,
        CASE 
            WHEN a.event_type = 'deposit' THEN silver.udf_get_account_pubkey_by_name('transferFrom', decoded_instruction:accounts)
            ELSE silver.udf_get_account_pubkey_by_name('stakeAuthority', decoded_instruction:accounts)
        END AS provider_address,
        CASE 
            WHEN a.event_type = 'depositStakeAccount' THEN c.post_amount
            ELSE (a.decoded_instruction:args:lamports::int) * pow(10, -9) 
        END AS deposit_amount,
        b.mint_amount * pow(10, -9) AS msol_minted,
        a.program_id,
        a._inserted_timestamp
    FROM
        base a
    LEFT JOIN mints b 
        ON a.tx_id = b.tx_id
        AND a.index = b.index
        AND b.mint = 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'
        AND COALESCE(b.inner_index, 0) > COALESCE(a.inner_index, -1)
        AND COALESCE(b.inner_index, 0) < COALESCE(a.next_liquid_staking_action_inner_index, 9999)
    LEFT JOIN sol_balances c
        ON a.tx_id = c.tx_id
        AND silver.udf_get_account_pubkey_by_name('stakeAccount', decoded_instruction:accounts) = c.account
        AND a.event_type = 'depositStakeAccount'
    WHERE
        a.event_type IN ('deposit', 'depositStakeAccount')
),
order_unstakes AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        index,
        inner_index,
        event_type AS action_type,
        silver.udf_get_account_pubkey_by_name('burnMsolAuthority', decoded_instruction:accounts) AS provider_address,
        (decoded_instruction:args:msolAmount::int) * pow(10, -9) AS msol_burned,
        program_id,
        _inserted_timestamp
    FROM
        base
    WHERE
        event_type = 'orderUnstake'
),
claims AS (
    SELECT
        a.block_id,
        a.block_timestamp, 
        a.tx_id,
        a.index,
        a.inner_index,
        a.event_type AS action_type,
        silver.udf_get_account_pubkey_by_name('transferSolTo', a.decoded_instruction:accounts) AS provider_address,
        b.amount AS claim_amount,
        a.program_id,
        a._inserted_timestamp
    FROM
        base a
    LEFT JOIN transfers b 
        ON a.tx_id = b.tx_id
        AND a.index = b.index
        AND COALESCE(b.inner_index, 0) > COALESCE(a.inner_index, -1)
        AND COALESCE(b.inner_index, 0) < COALESCE(a.next_liquid_staking_action_inner_index, 9999)
        AND b.tx_to = provider_address
    WHERE
        a.event_type = 'claim'
        AND b.mint = 'So11111111111111111111111111111111111111111'
)

SELECT
    block_id,
    block_timestamp,
    tx_id,
    index,
    inner_index,
    action_type,
    provider_address,
    deposit_amount,
    msol_minted,
    NULL AS msol_burned,
    NULL AS claim_amount,
    program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id', 'index', 'inner_index']) }} AS marinade_liquid_staking_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    deposits
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    index,
    inner_index,
    action_type,
    provider_address,
    NULL AS deposit_amount,
    NULL AS msol_minted,
    msol_burned,
    NULL AS claim_amount,
    program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id', 'index', 'inner_index']) }} AS marinade_liquid_staking_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    order_unstakes
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    index,
    inner_index,
    action_type,
    provider_address,
    NULL AS deposit_amount,
    NULL AS msol_minted,
    NULL AS msol_burned,
    claim_amount,
    program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id', 'index', 'inner_index']) }} AS marinade_liquid_staking_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    claims