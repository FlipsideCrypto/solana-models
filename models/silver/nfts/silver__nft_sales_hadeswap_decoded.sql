-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "nft_sales_hadeswap_decoded_id",
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
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
        INDEX,
        inner_index,
        program_id,
        event_type,
        decoded_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'
        AND event_type IN ('sellNftToLiquidityPair',
    'buyNftFromPair',
    'sellNftToTokenToNftPair')
        AND succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-10-30' -- TODO: refer to legacy model for sales before this date
{% endif %}
{% endset %}

{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.nft_sales_hadeswap_decoded__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}



with
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        program_id,
        
        case when event_type = 'sellNftToTokenToNftPair' 
                then solana_dev.silver.udf_get_account_pubkey_by_name('assetReceiver',decoded_instruction :accounts)
            when event_type = 'buyNftFromPair'
                then solana_dev.silver.udf_get_account_pubkey_by_name('user',decoded_instruction :accounts)
            when event_type = 'sellNftToLiquidityPair'
                then solana_dev.silver.udf_get_account_pubkey_by_name('nftsOwner',decoded_instruction :accounts)

        end AS buyer,
        
        case when event_type = 'sellNftToTokenToNftPair' 
                then solana_dev.silver.udf_get_account_pubkey_by_name('user',decoded_instruction :accounts)
            when event_type = 'buyNftFromPair'
                then solana_dev.silver.udf_get_account_pubkey_by_name('assetReceiver',decoded_instruction :accounts)
            when event_type = 'sellNftToLiquidityPair'
                then solana_dev.silver.udf_get_account_pubkey_by_name('user',decoded_instruction :accounts)
            
            
        end AS seller, -- main payment sent here

        case when event_type = 'sellNftToTokenToNftPair' 
                then solana_dev.silver.udf_get_account_pubkey_by_name('Remaining 1',decoded_instruction :accounts) 
            when event_type = 'buyNftFromPair'
                then solana_dev.silver.udf_get_account_pubkey_by_name('Remaining 1',decoded_instruction :accounts)
            when event_type = 'sellNftToLiquidityPair'
                then solana_dev.silver.udf_get_account_pubkey_by_name('Remaining 1',decoded_instruction :accounts)
        end AS fee_receiver, --fees sent here

        case when event_type = 'sellNftToTokenToNftPair' 
                then solana_dev.silver.udf_get_account_pubkey_by_name('fundsSolVault',decoded_instruction :accounts) 
            when event_type = 'buyNftFromPair'
                then solana_dev.silver.udf_get_account_pubkey_by_name('user',decoded_instruction :accounts)
            when event_type = 'sellNftToLiquidityPair'
                then solana_dev.silver.udf_get_account_pubkey_by_name('fundsSolVault',decoded_instruction :accounts)
                
        end AS buyer_escrow_vault, -- where payment comes from

 solana_dev.silver.udf_get_account_pubkey_by_name('nftMint',decoded_instruction :accounts) AS mint,
        _inserted_timestamp
    FROM
        silver.nft_sales_hadeswap_decoded__intermediate_tmp
)


,
transfers AS (
    SELECT
        A.*,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 1) :: INT, INDEX :: INT) AS index_1
    FROM
        {{ ref('silver__transfers') }} A
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                decoded
        ) d
        ON d.tx_id = A.tx_id
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
        A.buyer AS purchaser,
        A.seller,
        coalesce(0,C.amount) AS fee_amt,
        A.mint,
        A._inserted_timestamp,
        SUM(
            b.amount
        ) AS sale_amt,
    FROM
        decoded A
        LEFT JOIN transfers b
        ON A.tx_id = b.tx_id
        AND A.buyer_escrow_vault = b.tx_from
        AND A.seller = b.tx_to
        AND A.index = b.index_1
        LEFT JOIN transfers C
        ON A.tx_id = C.tx_id
        AND A.buyer_escrow_vault = C.tx_from
        AND A.fee_receiver = C.tx_to
        AND A.index = C.index_1
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    seller,
    mint,
    sale_amt + fee_amt AS sales_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','mint']) }} AS nft_sales_hadeswap_decoded_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final