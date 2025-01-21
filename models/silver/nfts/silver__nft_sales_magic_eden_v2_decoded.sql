-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "nft_sales_magic_eden_v2_decoded_id",
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.nft_sales_magic_eden_v2_decoded__intermediate_tmp AS
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        signers,
        index,
        inner_index,
        program_id,
        event_type,
        decoded_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
        AND event_type IN ('mip1ExecuteSaleV2', 'executeSaleV2', 'ocpExecuteSaleV2')
        AND succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT 
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM 
        {{ this }}
)
{% else %}
        AND block_timestamp::DATE > '2024-10-01' -- temp test but update to where decoded instructions are good
{% endif %}
{% endset %}

{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.nft_sales_magic_eden_v2_decoded__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH base AS (
    SELECT 
        * 
    FROM 
        silver.nft_sales_magic_eden_v2_decoded__intermediate_tmp
),

decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        program_id,
        solana_dev.silver.udf_get_account_pubkey_by_name('buyer', decoded_instruction:accounts) AS purchaser,
        solana_dev.silver.udf_get_account_pubkey_by_name('seller', decoded_instruction:accounts) AS seller,
        CASE
            WHEN event_type in ('executeSaleV2') THEN solana_dev.silver.udf_get_account_pubkey_by_name('escrowPaymentAccount', decoded_instruction:accounts) 
            else solana_dev.silver.udf_get_account_pubkey_by_name('buyerEscrowPaymentAccount', decoded_instruction:accounts) 
        end as buyer_payment_acct,
        solana_dev.silver.udf_get_account_pubkey_by_name('tokenMint', decoded_instruction:accounts) AS mint,
        _inserted_timestamp
    FROM
        base
),

transfers AS (
    SELECT
        a.block_timestamp,
        a.tx_id,          
        a.tx_from,       
        a.tx_to,
        a.amount,
        a.mint,         
        a.succeeded,      
        a.index,
        COALESCE(SPLIT_PART(a.index::text, '.', 1)::INT, a.index::INT) AS index_1,
        a._inserted_timestamp 
    FROM
        solana_dev.silver.transfers a
    INNER JOIN (
        SELECT DISTINCT tx_id
        FROM decoded
    ) d ON d.tx_id = a.tx_id
    WHERE
        a.succeeded
        AND {{ between_stmts }}
),

pre_final AS (
    SELECT
        a.block_id,
        a.block_timestamp,
        a.program_id,
        a.tx_id,
        b.succeeded,
        a.index,
        a.inner_index,
        a.purchaser,
        a.seller,
        a.mint,
        b.mint as currency_mint,
        a._inserted_timestamp,
        SUM(b.amount) AS sales_amount
    FROM
        decoded a
    LEFT JOIN transfers b
    ON a.tx_id = b.tx_id
    AND a.buyer_payment_acct = b.tx_from
    AND a.index = b.index_1
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    currency_mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id', 'a.mint']) }} AS nft_sales_hadeswap_decoded_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final a