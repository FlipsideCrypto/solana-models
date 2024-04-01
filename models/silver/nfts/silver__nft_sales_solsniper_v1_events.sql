{{ config(
    materialized = 'table',
    unique_key = ['nft_sales_solsniper_id'],
    cluster_by = ['block_timestamp::DATE'],
    tags=['test_weekly']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'SNPRohhBurQwrpwAptw1QYtpFdfEKitr4WSJ125cN1g'
        AND event_type IN (
            'withdrawAndCloseBuyOrderLock',
            'buyMeListingV1',
            'buyMeMip1ListingV1',
            'buyTensorSingleListingV1',
            'buyTensorPoolListingV1'
        )
        AND block_timestamp :: DATE BETWEEN '2023-05-26'
        AND '2024-01-30'
),
decoded AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.index,
        A.inner_index,
        A.program_id,
        A.event_type AS event_type_temp,
        silver.udf_get_account_pubkey_by_name(
            'buyer',
            b.decoded_instruction :accounts
        ) AS buyer,
        CASE
            WHEN A.event_type = 'buyMeListingV1' THEN silver.udf_get_account_pubkey_by_name(
                'Remaining 0',
                A.decoded_instruction :accounts
            )
            WHEN A.event_type = 'buyMeMip1ListingV1' THEN silver.udf_get_account_pubkey_by_name(
                'Remaining 4',
                A.decoded_instruction :accounts
            )
            WHEN A.event_type = 'buyTensorPoolListingV1' THEN silver.udf_get_account_pubkey_by_name(
                'tswapPoolOwner',
                A.decoded_instruction :accounts
            )
            WHEN A.event_type = 'buyTensorSingleListingV1' THEN silver.udf_get_account_pubkey_by_name(
                'seller',
                A.decoded_instruction :accounts
            )
        END AS seller,
        CASE
            WHEN A.event_type = 'buyMeListingV1' THEN silver.udf_get_account_pubkey_by_name(
                'Remaining 5',
                A.decoded_instruction :accounts
            )
            WHEN A.event_type = 'buyMeMip1ListingV1' THEN silver.udf_get_account_pubkey_by_name(
                'Remaining 10',
                A.decoded_instruction :accounts
            )
        END AS payment_acct,  -- sent here to distribute to seller and fee for ME
        CASE
            WHEN A.event_type = 'buyTensorPoolListingV1' THEN silver.udf_get_account_pubkey_by_name(
                'tswapSolEscrow',
                A.decoded_instruction :accounts
            )
            ELSE NULL
        END AS tensor_sol_escrow,
        silver.udf_get_account_pubkey_by_name(
            'buyerEscrowVault',
            A.decoded_instruction :accounts
        ) AS buyer_escrow_vault, -- where payment comes from
        silver.udf_get_account_pubkey_by_name(
            'nftMint',
            A.decoded_instruction :accounts
        ) AS mint,
        A._inserted_timestamp
    FROM
        base A
        LEFT JOIN base b
        ON A.tx_id = b.tx_id
    WHERE
        A.event_type IN (
            'buyMeListingV1',
            'buyMeMip1ListingV1',
            'buyTensorSingleListingV1',
            'buyTensorPoolListingV1'
        )
        AND b.event_type = 'withdrawAndCloseBuyOrderLock'
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
                DISTINCT tx_id
            FROM
                decoded
        ) d
        ON d.tx_id = A.tx_id
    WHERE
        A.succeeded
        AND A.block_timestamp :: DATE BETWEEN '2023-05-26'
        AND '2024-01-30'
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
        A.mint,
        A._inserted_timestamp,
        b.amount AS sales_amount
    FROM
        decoded A
        INNER JOIN transfers b
        ON A.tx_id = b.tx_id
        AND A.buyer_escrow_vault = b.tx_from
        AND A.payment_acct = b.tx_to
        AND A.index = b.index_1
        AND A.event_type_temp IN (
            'buyMeListingV1',
            'buyMeMip1ListingV1'
        )
    UNION ALL
    SELECT
        A.block_id,
        A.block_timestamp,
        A.program_id,
        A.tx_id,
        b.succeeded,
        A.buyer AS purchaser,
        A.seller,
        A.mint,
        A._inserted_timestamp,
        SUM(
            b.amount
        ) AS sales_amount,
    FROM
        decoded A
        INNER JOIN transfers b
        ON A.tx_id = b.tx_id
        AND A.buyer_escrow_vault = b.tx_from
        AND (
            A.seller = b.tx_to
            OR A.tensor_sol_escrow = b.tx_to
        )
        AND A.index = b.index_1
    WHERE
        A.event_type_temp IN (
            'buyTensorSingleListingV1',
            'buyTensorPoolListingV1'
        )
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9
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
    sales_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','mint']) }} AS nft_sales_solsniper_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final
