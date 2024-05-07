-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "nft_sales_tensorswap_id",
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE temporary TABLE silver.nft_sales_tensorswap__intermediate_tmp AS
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        inner_index,
        program_id,
        event_type,
        decoded_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
        AND event_type IN (
            'wnsBuySingleListing',
            'buyNftT22',
            'buySingleListing',
            'buyNft',
            'buySingleListingT22',
            'wnsBuyNft',
            'sellNftTokenPool',
            'sellNftTokenPoolT22',
            'sellNftTradePoolT22',
            'wnsSellNftTradePool',
            'wnsSellNftTokenPool',
            'sellNftTradePool'
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
        AND _inserted_timestamp :: DATE >= '2023-11-15'
    {% endif %}
{% endset %}

{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.nft_sales_tensorswap__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        inner_index,
        program_id,
        event_type,
        CASE
            WHEN event_type IN (
                'wnsBuySingleListing',
                'buyNftT22',
                'buySingleListing',
                'buyNft',
                'buySingleListingT22',
                'wnsBuyNft'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'buyer',
                decoded_instruction :accounts
            )
            WHEN event_type IN (
                'sellNftTokenPool',
                'sellNftTokenPoolT22',
                'sellNftTradePoolT22',
                'wnsSellNftTradePool',
                'wnsSellNftTokenPool',
                'sellNftTradePool'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'shared > owner',
                decoded_instruction :accounts
            )
        END AS purchaser,
        CASE
            WHEN event_type IN (
                'wnsBuySingleListing',
                'buyNftT22',
                'buySingleListing',
                'buyNft',
                'buySingleListingT22',
                'wnsBuyNft'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'owner',
                decoded_instruction :accounts
            )
            WHEN event_type IN (
                'sellNftTokenPool',
                'sellNftTokenPoolT22',
                'sellNftTradePoolT22',
                'wnsSellNftTradePool',
                'wnsSellNftTokenPool',
                'sellNftTradePool'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'shared > seller',
                decoded_instruction :accounts
            )
        END AS seller,
        CASE
            WHEN event_type IN (
                'wnsBuySingleListing',
                'buyNftT22',
                'buySingleListing',
                'buyNft',
                'buySingleListingT22',
                'wnsBuyNft'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'nftMint',
                decoded_instruction :accounts
            )
            WHEN event_type IN (
                'sellNftTokenPool',
                'sellNftTokenPoolT22',
                'sellNftTradePoolT22',
                'wnsSellNftTradePool',
                'wnsSellNftTokenPool',
                'sellNftTradePool'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'shared > nftMint',
                decoded_instruction :accounts
            )
        END AS mint,
        CASE
            WHEN event_type IN (
                'sellNftTokenPool',
                'sellNftTokenPoolT22',
                'wnsSellNftTradePool',
                'wnsSellNftTokenPool',
                'sellNftTradePool',
                'sellNftTradePoolT22'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'shared > solEscrow',
                decoded_instruction :accounts
            )
        END AS sol_escrow,
        CASE 
            WHEN event_type IN ('sellNftTradePoolT22') THEN silver.udf_get_account_pubkey_by_name(
                'marginAccount',
                decoded_instruction :accounts
            )
        END AS margin_account,
        decoded_instruction:args:minPrice::int * pow(10,-9) AS min_price,
        _inserted_timestamp
    FROM
        silver.nft_sales_tensorswap__intermediate_tmp
),
base_balances AS (
    SELECT
        t.block_timestamp,
        t.tx_id,
        t.signers,
        t.pre_balances,
        t.post_balances,
        t.account_keys
    FROM
        {{ ref('silver__transactions') }} t
        JOIN decoded d USING(tx_id)
    WHERE
        {{ between_stmts }}
        AND d.event_type IN (
            'sellNftTokenPool',
            'sellNftTokenPoolT22',
            'sellNftTradePoolT22',
            'wnsSellNftTradePool',
            'wnsSellNftTokenPool',
            'sellNftTradePool'
        )
),
base_transfers AS (
    SELECT 
        block_timestamp,
        tx_id,
        index,
        tx_from,
        mint,
        amount
    FROM
        {{ ref('silver__transfers') }} t
    WHERE
        {{ between_stmts }}
),
buys AS (
    SELECT
        d.block_timestamp,
        d.tx_id,
        d.index,
        SUM(
            t.amount
        ) AS sales_amount
    FROM
        base_transfers t
        JOIN decoded d
        ON d.block_timestamp :: DATE = t.block_timestamp :: DATE
        AND d.tx_id = t.tx_id
        AND d.index = SPLIT_PART(
            t.index,
            '.',
            1
        )
        AND d.purchaser = t.tx_from
    WHERE
        d.event_type IN (
            'wnsBuySingleListing',
            'buyNftT22',
            'buySingleListing',
            'buyNft',
            'buySingleListingT22',
            'wnsBuyNft'
        )
        AND t.mint = 'So11111111111111111111111111111111111111112'

    GROUP BY
        1,2,3
),
sells AS (
    SELECT
        d.block_timestamp,
        d.tx_id,
        d.index,
        t.pre_balances,
        t.post_balances,
        t.account_keys,
        silver.udf_get_account_balances_index(
            d.sol_escrow,
            t.account_keys
        ) AS escrow_balance_index,
        silver.udf_get_account_balances_index(
            d.margin_account,
            t.account_keys
        ) AS margin_balance_index,
        (t.pre_balances [escrow_balance_index] - t.post_balances [escrow_balance_index]) * pow(10,-9) AS sales_amount, /* sale always in sol, assuming 9 decimals */
        (t.pre_balances [margin_balance_index] - t.post_balances [margin_balance_index]) * pow(10,-9) AS sales_amount2,
        min_price,
    FROM
        base_balances t
        JOIN decoded d
        ON d.block_timestamp :: DATE = t.block_timestamp :: DATE
        AND d.tx_id = t.tx_id
    WHERE
        d.event_type IN (
            'sellNftTokenPool',
            'sellNftTokenPoolT22',
            'sellNftTradePoolT22',
            'wnsSellNftTradePool',
            'wnsSellNftTokenPool',
            'sellNftTradePool'
        )
),
pre_final AS (
    SELECT
        d.block_timestamp,
        d.block_id,
        d.tx_id,
        d.succeeded,
        d.index,
        d.inner_index,
        d.program_id,
        d.purchaser,
        d.seller,
        d.mint,
        case 
            when coalesce(s.sales_amount,0) <> 0 then 
                s.sales_amount
            when coalesce(s.sales_amount2,0) <> 0 then 
                s.sales_amount2
            when coalesce(s.min_price,0) <> 0 then
                s.min_price
        end as sales_amount,
        d._inserted_timestamp,
    FROM
        decoded d
        JOIN sells s 
        USING(block_timestamp, tx_id, index)
    UNION ALL
    SELECT
        d.block_timestamp,
        d.block_id,
        d.tx_id,
        d.succeeded,
        d.index,
        d.inner_index,
        d.program_id,
        d.purchaser,
        d.seller,
        d.mint,
        b.sales_amount,
        d._inserted_timestamp,
    FROM
        decoded d
        JOIN buys b 
        USING(block_timestamp, tx_id, index)
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index','mint']) }} AS nft_sales_tensorswap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM 
    pre_final