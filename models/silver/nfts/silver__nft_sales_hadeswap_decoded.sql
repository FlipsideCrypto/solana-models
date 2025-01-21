-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "nft_sales_hadeswap_decoded_id",
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE temporary TABLE silver.nft_sales_hadeswap_decoded__intermediate_tmp AS
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
        program_id = 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'
        AND event_type IN ('sellNftToLiquidityPair', 'buyNftFromPair', 'sellNftToTokenToNftPair')
        AND succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE > '2023-02-08' --  decoding returns errors until this date
{% endif %}
{% endset %}

{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.nft_sales_hadeswap_decoded__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        program_id,
        CASE
            WHEN event_type = 'sellNftToTokenToNftPair' THEN silver.udf_get_account_pubkey_by_name('assetReceiver', decoded_instruction:accounts)
            WHEN event_type = 'buyNftFromPair' THEN silver.udf_get_account_pubkey_by_name('user', decoded_instruction:accounts)
            WHEN event_type = 'sellNftToLiquidityPair' THEN silver.udf_get_account_pubkey_by_name('nftsOwner', decoded_instruction:accounts)
        END AS purchaser,
        CASE
            WHEN event_type = 'sellNftToTokenToNftPair' THEN silver.udf_get_account_pubkey_by_name('user', decoded_instruction:accounts)
            WHEN event_type = 'buyNftFromPair' THEN silver.udf_get_account_pubkey_by_name('nftsOwner', decoded_instruction:accounts)
            WHEN event_type = 'sellNftToLiquidityPair' THEN silver.udf_get_account_pubkey_by_name('user', decoded_instruction:accounts)
        END AS seller,
        CASE
            WHEN event_type = 'sellNftToTokenToNftPair' THEN silver.udf_get_account_pubkey_by_name('fundsSolVault', decoded_instruction:accounts)
            WHEN event_type = 'buyNftFromPair' THEN silver.udf_get_account_pubkey_by_name('user', decoded_instruction:accounts)
            WHEN event_type = 'sellNftToLiquidityPair' THEN silver.udf_get_account_pubkey_by_name('fundsSolVault', decoded_instruction:accounts)
        END AS buyer_escrow_vault,
        silver.udf_get_account_pubkey_by_name('nftMint', decoded_instruction:accounts) AS mint,
        _inserted_timestamp
    FROM
        silver.nft_sales_hadeswap_decoded__intermediate_tmp
),
transfers AS (
    SELECT
        a.block_timestamp,
        a.tx_id,          
        a.tx_from,       
        a.tx_to,
        a.amount,         
        a.succeeded,      
        a.index,
        COALESCE(SPLIT_PART(a.INDEX :: text, '.', 1) :: INT, a.INDEX :: INT) AS index_1,
        a._inserted_timestamp 
    FROM
        {{ ref('silver__transfers') }} A
    INNER JOIN (
        SELECT DISTINCT tx_id
        FROM decoded
    ) d ON d.tx_id = A.tx_id
    WHERE
        A.succeeded
        AND {{ between_stmts }}
),
pre_final AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.program_id,
        A.tx_id,
        b.succeeded,
        A.index,
        A.inner_index,
        A.purchaser,
        A.seller,
        A.mint,
        A._inserted_timestamp,
        sum(b.amount) AS sales_amount
    FROM
        decoded A
    LEFT JOIN transfers b
    ON A.tx_id = b.tx_id
    AND A.buyer_escrow_vault = b.tx_from
    AND A.index = b.index_1
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
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
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id', 'mint']) }} AS nft_sales_hadeswap_decoded_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
qualify(row_number() over (partition by tx_id, mint order by index desc)) = 1