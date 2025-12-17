-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['lending_kamino_repays_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.lending_kamino_repays__tmp AS
        SELECT
            *
        FROM
            {{ ref('silver__decoded_instructions_combined') }}
        WHERE
            program_id = 'KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD'
            AND event_type IN (
            'repayObligationLiquidity','repayObligationLiquidityV2','flashRepayReserveLiquidity'
            )
            AND succeeded
        {% if is_incremental() %}
            AND _inserted_timestamp >= (
                SELECT
                    MAX(_inserted_timestamp) - INTERVAL '1 hour'
                FROM
                    {{ this }}
            )
        {% else %}
            AND _inserted_timestamp::DATE >= '2025-03-07'
        {% endif %}
    {% endset %}
    
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate(
        "silver.lending_kamino_repays__tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.lending_kamino_repays__tmp
),

decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        program_id,
        event_type,
-- PAYER (the address making the payment)
CASE 
    WHEN event_type = 'repayObligationLiquidityV2' 
    THEN silver.udf_get_account_pubkey_by_name('repayAccounts > owner', decoded_instruction:accounts)
    
    WHEN event_type = 'repayObligationLiquidity'
    THEN silver.udf_get_account_pubkey_by_name('owner', decoded_instruction:accounts)
    
    WHEN event_type = 'flashRepayReserveLiquidity'
    THEN silver.udf_get_account_pubkey_by_name('userTransferAuthority', decoded_instruction:accounts)
END AS payer,
-- BORROWER (the address whose debt is being repaid - same as payer)
CASE 
    WHEN event_type = 'repayObligationLiquidityV2' 
    THEN silver.udf_get_account_pubkey_by_name('repayAccounts > owner', decoded_instruction:accounts)
    
    WHEN event_type = 'repayObligationLiquidity'
    THEN silver.udf_get_account_pubkey_by_name('owner', decoded_instruction:accounts)
    
    WHEN event_type = 'flashRepayReserveLiquidity'
    THEN silver.udf_get_account_pubkey_by_name('userTransferAuthority', decoded_instruction:accounts)
END AS borrower,
-- PROTOCOL_MARKET (collateral mint - NOT available in repay events)
NULL AS protocol_market,
-- TOKEN_ADDRESS (the underlying token being repaid)
CASE 
    WHEN event_type = 'repayObligationLiquidityV2' 
    THEN silver.udf_get_account_pubkey_by_name('repayAccounts > reserveLiquidityMint', decoded_instruction:accounts)
    
    WHEN event_type = 'repayObligationLiquidity'
    THEN silver.udf_get_account_pubkey_by_name('reserveLiquidityMint', decoded_instruction:accounts)
    
    WHEN event_type = 'flashRepayReserveLiquidity'
    THEN silver.udf_get_account_pubkey_by_name('reserveLiquidityMint', decoded_instruction:accounts)
END AS token_address,
CASE 
    WHEN event_type = 'repayObligationLiquidityV2' 
    THEN silver.udf_get_account_pubkey_by_name('repayAccounts > userSourceLiquidity', decoded_instruction:accounts)

    else silver.udf_get_account_pubkey_by_name('userSourceLiquidity', decoded_instruction:accounts)
    
END AS source_token_account,

CASE 
    WHEN event_type = 'repayObligationLiquidityV2' 
    THEN silver.udf_get_account_pubkey_by_name('repayAccounts > reserveDestinationLiquidity', decoded_instruction:accounts)

    else silver.udf_get_account_pubkey_by_name('reserveDestinationLiquidity', decoded_instruction:accounts)
    
END AS dest_token_account,
        _inserted_timestamp
    FROM
        base
),
transfers AS (
    SELECT
        A.*,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 1) :: INT, INDEX :: INT) AS index_1,
        NULLIF(SPLIT_PART(INDEX :: text, '.', 2), '') :: INT AS inner_index_1
    FROM
        {{ ref('silver__transfers') }} A
        INNER JOIN (
            SELECT
                DISTINCT tx_id,
                    block_timestamp::DATE AS block_date
            FROM
                decoded
        ) d
        ON d.block_date = A.block_timestamp::DATE
        AND d.tx_id = A.tx_id
    WHERE
        A.succeeded
        and {{ between_stmts }}
),
token_decimals AS (
    SELECT 
        mint,
        decimal
    FROM
        {{ ref('silver__decoded_metadata') }}
    UNION ALL 
    SELECT 
        'So11111111111111111111111111111111111111112',
        9
    UNION ALL 
    SELECT 
        'GyD5AvrcZAhSP5rrhXXGPUHri6sbkRpq67xfG3x8ourT',
        9
)

SELECT 
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    a.index,
    a.inner_index,
    a.program_id,
    a.event_type,
    a.payer,
    a.borrower,
    a.protocol_market,
    a.token_address,
    t.amount * POW(10, b.decimal) as amount_raw,
    t.amount as amount,
    b.decimal,
    a._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id', 'a.index', 'COALESCE(a.inner_index, -1)']) }} AS lending_kamino_repays_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    decoded a
left join transfers t
    ON A.tx_id = t.tx_id
    AND A.index = t.index_1
    AND A.borrower = t.tx_from
    AND A.source_token_account = t.source_token_account
    AND A.dest_token_account = t.dest_token_account
LEFT JOIN token_decimals b
    ON a.token_address = b.mint
where t.amount is not null
