-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['lending_kamino_withdraws_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.lending_kamino_withdraws__tmp AS
        SELECT
            *
        FROM
            {{ ref('silver__decoded_instructions_combined') }}
        WHERE
            program_id = 'KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD'
            AND event_type IN ('withdrawObligationCollateralAndRedeemReserveCollateralV2','withdrawObligationCollateralAndRedeemReserveCollateral')
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
        "silver.lending_kamino_withdraws__tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.lending_kamino_withdraws__tmp
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
            WHEN event_type = 'withdrawObligationCollateralAndRedeemReserveCollateralV2' 
            THEN silver.udf_get_account_pubkey_by_name('withdrawAccounts > owner', decoded_instruction:accounts)
            ELSE silver.udf_get_account_pubkey_by_name('owner', decoded_instruction:accounts)
        END AS depositor,
        CASE 
            WHEN event_type = 'withdrawObligationCollateralAndRedeemReserveCollateralV2' 
            THEN silver.udf_get_account_pubkey_by_name('withdrawAccounts > reserveCollateralMint', decoded_instruction:accounts)
            ELSE silver.udf_get_account_pubkey_by_name('reserveCollateralMint', decoded_instruction:accounts)
        END AS protocol_market,
        CASE 
            WHEN event_type = 'withdrawObligationCollateralAndRedeemReserveCollateralV2' 
            THEN silver.udf_get_account_pubkey_by_name('withdrawAccounts > reserveLiquidityMint', decoded_instruction:accounts)
            ELSE silver.udf_get_account_pubkey_by_name('reserveLiquidityMint', decoded_instruction:accounts)
        END AS token_address,
        decoded_instruction:args:collateralAmount::int AS amount_raw,
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
    a.depositor,
    a.protocol_market,
    a.token_address,
    a.amount_raw,
    a.amount_raw * POW(10, -b.decimal) AS amount,
    b.decimal,
    a._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id', 'a.index', 'COALESCE(a.inner_index, -1)']) }} AS lending_kamino_withdraws_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    decoded a
LEFT JOIN token_decimals b
    ON a.token_address = b.mint
