-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "nft_sales_amm_sell_decoded_id",
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE temporary TABLE silver.nft_sales_amm_sell_decoded__intermediate_tmp AS

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
        program_id = 'mmm3XBJg5gk8XJxEKBvdgptZz6SgK4tXvn36sodowMc'
        AND event_type IN (
            'solMip1FulfillSell',
            'solFulfillSell',
            'solFulfillBuy',
            'solMip1FulfillBuy',
            'solOcpFulfillBuy',
            'solExtFulfillBuy',
            'solOcpFulfillSell'
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
    AND block_timestamp :: DATE >= '2022-10-30' -- refer to legacy model for sales before this date
{% endif %}
{% endset %}

{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.nft_sales_amm_sell_decoded__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH base_decoded AS (
    SELECT
        *
    FROM
        silver.nft_sales_amm_sell_decoded__intermediate_tmp
),
base_transfers AS (
    SELECT
        *
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        mint = 'So11111111111111111111111111111111111111111'
        AND succeeded
        AND {{ between_stmts }}
),
coral_cube_sales AS(
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        signers,
        succeeded,
        program_id,
        decoded_instruction,
        event_type,
        CASE
            WHEN event_type IN (
                'solFulfillSell',
                'solMip1FulfillSell',
                'solOcpFulfillSell'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'payer',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'pool',
                decoded_instruction :accounts
            )
        END AS purchaser,
        CASE
            WHEN event_type IN (
                'solFulfillSell',
                'solMip1FulfillSell',
                'solOcpFulfillSell'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'pool',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'payer',
                decoded_instruction :accounts
            )
        END AS seller,
        CASE
            WHEN event_type IN (
                'solFulfillSell',
                'solMip1FulfillSell',
                'solOcpFulfillSell'
            ) THEN 'sell'
            ELSE 'buy'
        END AS nft_sale_type,
        'Coral Cube' AS marketplace,
        silver.udf_get_account_pubkey_by_name(
            'assetMint',
            decoded_instruction :accounts
        ) AS mint,
        _inserted_timestamp
    FROM
        base_decoded
    WHERE
        signers [1] = '7RpRDUZBdu5hfmqWvobPazbNeVCagRk5E3Rb8Bm8qRmD'
        AND purchaser <> '7RpRDUZBdu5hfmqWvobPazbNeVCagRk5E3Rb8Bm8qRmD'
),
mev2_sales AS(
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        signers,
        succeeded,
        program_id,
        decoded_instruction,
        event_type,
        CASE
            WHEN event_type IN (
                'solMip1FulfillSell',
                'solFulfillSell'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'payer',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'owner',
                decoded_instruction :accounts
            )
        END AS purchaser,
        CASE
            WHEN event_type IN (
                'solMip1FulfillSell',
                'solFulfillSell'
            ) THEN silver.udf_get_account_pubkey_by_name(
                'owner',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'payer',
                decoded_instruction :accounts
            )
        END AS seller,
        CASE
            WHEN event_type IN (
                'solMip1FulfillSell',
                'solFulfillSell'
            ) THEN 'sell'
            ELSE 'buy'
        END AS nft_sale_type,
        'Magic Eden' AS marketplace,
        silver.udf_get_account_pubkey_by_name(
            'assetMint',
            decoded_instruction :accounts
        ) AS mint,
        _inserted_timestamp
    FROM
        base_decoded
    WHERE
        signers [1] = 'NTYeYJ1wr4bpM5xo6zx5En44SvJFAd35zTxxNoERYqd'
),
coral_cube_nft_sale_amount AS (
    SELECT
        A.tx_id,
        b.mint,
        SUM(
            b.amount
        ) AS sales_amount
    FROM
        coral_cube_sales A
        LEFT OUTER JOIN base_transfers b
        ON A.tx_id = b.tx_id
    WHERE
        (
            A.nft_sale_type = 'sell'
            AND A.purchaser = b.tx_from
        )
        OR (
            A.nft_sale_type = 'buy'
            AND silver.udf_get_account_pubkey_by_name(
                'buysideSolEscrowAccount',
                A.decoded_instruction :accounts
            ) :: STRING = b.tx_from
        )
    GROUP BY
        1,
        2
),
mev2_nft_sale_amount AS (
    SELECT
        A.tx_id,
        b.mint,
        SUM(
            b.amount
        ) AS sales_amount
    FROM
        mev2_sales A
        LEFT OUTER JOIN base_transfers b
        ON A.tx_id = b.tx_id
    WHERE
        (
            A.nft_sale_type = 'sell'
            AND A.purchaser = b.tx_from
        )
        OR (
            A.nft_sale_type = 'buy'
            AND silver.udf_get_account_pubkey_by_name(
                'buysideSolEscrowAccount',
                A.decoded_instruction :accounts
            ) :: STRING = b.tx_from
        )
    GROUP BY
        1,
        2
),
pre_final AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.index,
        A.inner_index,
        A.program_id,
        A.mint,
        A.purchaser,
        A.seller,
        b.sales_amount,
        A.marketplace,
        A._inserted_timestamp
    FROM
        coral_cube_sales A
        LEFT JOIN coral_cube_nft_sale_amount b
        ON A.tx_id = b.tx_id
    WHERE
        b.sales_amount IS NOT NULL
    UNION
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.index,
        A.inner_index,
        A.program_id,
        A.mint,
        A.purchaser,
        A.seller,
        b.sales_amount,
        A.marketplace,
        A._inserted_timestamp
    FROM
        mev2_sales A
        LEFT JOIN mev2_nft_sale_amount b
        ON A.tx_id = b.tx_id
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    mint,
    purchaser,
    seller,
    sales_amount,
    marketplace,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','mint']) }} AS nft_sales_amm_sell_decoded_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final

