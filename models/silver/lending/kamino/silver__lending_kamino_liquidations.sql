-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['lending_kamino_liquidations_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.lending_kamino_liquidations__tmp AS
        SELECT
            *
        FROM
            {{ ref('silver__decoded_instructions_combined') }}
        WHERE
            program_id = 'KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD'
            AND event_type IN (
                'liquidateObligationAndRedeemReserveCollateralV2',
                'liquidateObligationAndRedeemReserveCollateral'
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
        "silver.lending_kamino_liquidations__tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.lending_kamino_liquidations__tmp
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

        CASE 
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateralV2' 
            THEN silver.udf_get_account_pubkey_by_name('liquidationAccounts > liquidator', decoded_instruction:accounts)
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateral'
            THEN silver.udf_get_account_pubkey_by_name('liquidator', decoded_instruction:accounts)
        END AS liquidator,
        
        CASE 
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateralV2' 
            THEN silver.udf_get_account_pubkey_by_name('liquidationAccounts > withdrawReserveLiquidityMint', decoded_instruction:accounts)
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateral'
            THEN silver.udf_get_account_pubkey_by_name('withdrawReserveLiquidityMint', decoded_instruction:accounts)
        END AS collateral_token,
        
        CASE 
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateralV2' 
            THEN silver.udf_get_account_pubkey_by_name('liquidationAccounts > repayReserveLiquidityMint', decoded_instruction:accounts)
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateral'
            THEN silver.udf_get_account_pubkey_by_name('repayReserveLiquidityMint', decoded_instruction:accounts)
        END AS debt_token,
        CASE 
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateralV2' 
            THEN silver.udf_get_account_pubkey_by_name('liquidationAccounts > withdrawReserveCollateralMint', decoded_instruction:accounts)
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateral'
            THEN silver.udf_get_account_pubkey_by_name('withdrawReserveCollateralMint', decoded_instruction:accounts)
        END AS protocol_market,
        CASE 
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateralV2' 
            THEN silver.udf_get_account_pubkey_by_name('liquidationAccounts > obligation', decoded_instruction:accounts)
            
            WHEN event_type = 'liquidateObligationAndRedeemReserveCollateral'
            THEN silver.udf_get_account_pubkey_by_name('obligation', decoded_instruction:accounts)
        END AS obligation,
        decoded_instruction:args:liquidityAmount::INT AS amount_raw,
        _inserted_timestamp
    FROM
        base
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
    a.liquidator,
    c.owner as borrower,
    a.obligation,
    a.collateral_token,
    a.debt_token,
    a.protocol_market,
    a.amount_raw,
    a.amount_raw * POW(10, -b.decimal) AS amount,
    b.decimal,
    a._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id', 'a.index', 'COALESCE(a.inner_index, -1)']) }} AS lending_kamino_liquidations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    decoded a
LEFT JOIN token_decimals b
    ON a.debt_token = b.mint
LEFT JOIN {{ ref('silver__lending_kamino_obligations') }} c
    ON a.obligation = c.obligation