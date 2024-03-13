{{ config(
    materialized = 'table',
    unique_key = ['nft_sales_solsniper_id'],
) }}

WITH base AS (

    SELECT
        *
    FROM
        solana.silver.decoded_instructions_combined
    WHERE
        program_id = 'SNPRohhBurQwrpwAptw1QYtpFdfEKitr4WSJ125cN1g'
        AND event_type in ('withdrawAndCloseBuyOrderLock','buyMeListingV1','buyMeMip1ListingV1','buyTensorSingleListingV1','buyTensorPoolListingV1')
        and block_timestamp::date between '2023-05-26' and '2024-01-30'
)

,
decoded AS (
    SELECT
        a.block_timestamp,
        a.block_id,
        a.tx_id,
        a.INDEX,
        a.inner_index,
        a.program_id,
        a.event_type as event_type_temp,
        silver.udf_get_account_pubkey_by_name('buyer',b.decoded_instruction :accounts) AS buyer,
        case 
            when a.event_type = 'buyMeListingV1'
            then silver.udf_get_account_pubkey_by_name('Remaining 0',a.decoded_instruction :accounts) 
            when a.event_type = 'buyMeMip1ListingV1'
            then silver.udf_get_account_pubkey_by_name('Remaining 4',a.decoded_instruction :accounts) 
            when a.event_type = 'buyTensorPoolListingV1'
            then silver.udf_get_account_pubkey_by_name('tswapPoolOwner',a.decoded_instruction :accounts) 
            when a.event_type = 'buyTensorSingleListingV1'
            then silver.udf_get_account_pubkey_by_name('seller',a.decoded_instruction :accounts) 
            end AS seller,
        case 
            when a.event_type = 'buyMeListingV1'
            then silver.udf_get_account_pubkey_by_name('Remaining 5',a.decoded_instruction :accounts) -- sent here to distribute to seller and fee for ME
            when a.event_type = 'buyMeMip1ListingV1'
            then silver.udf_get_account_pubkey_by_name('Remaining 10',a.decoded_instruction :accounts) 
            end AS payment_acct,
        case 
            when a.event_type = 'buyTensorPoolListingV1'
            then silver.udf_get_account_pubkey_by_name('tswapSolEscrow',a.decoded_instruction :accounts)
            else null
            end AS tensor_sol_escrow,
        silver.udf_get_account_pubkey_by_name('buyerEscrowVault',a.decoded_instruction :accounts) AS buyer_escrow_vault, -- where payment comes from
        silver.udf_get_account_pubkey_by_name('nftMint',a.decoded_instruction :accounts) AS mint,
        a._inserted_timestamp
    FROM
        base a
    left join base b
    on a.tx_id = b.tx_id
    where a.event_type in ('buyMeListingV1','buyMeMip1ListingV1','buyTensorSingleListingV1','buyTensorPoolListingV1')
    and b.event_type = 'withdrawAndCloseBuyOrderLock'
    
)
,
transfers AS (
    SELECT
        A.*,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 1) :: INT, INDEX :: INT) AS index_1,
        NULLIF(SPLIT_PART(INDEX :: text, '.', 2), '') :: INT AS inner_index_1
    FROM
        solana.silver.transfers A
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                decoded
        ) d
        ON d.tx_id = A.tx_id
    WHERE
        A.succeeded
        and A.block_timestamp::date between '2023-05-26' and '2024-01-30'
)
-- select * from transfers;
,
pre_final AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.program_id,
        A.tx_id,
        b.succeeded,
        A.buyer as purchaser,
        a.seller,
        a.mint,
        A._inserted_timestamp,
        b.amount as sales_amount
    FROM
        decoded A
        inner JOIN transfers b
        ON A.tx_id = b.tx_id
        and A.buyer_escrow_vault = b.tx_from
        and a.payment_acct = b.tx_to
        AND A.index = b.index_1
        and a.event_type_temp in ('buyMeListingV1','buyMeMip1ListingV1')
union all
    SELECT
        A.block_id,
        A.block_timestamp,
        A.program_id,
        A.tx_id,
        b.succeeded,
        A.buyer as purchaser,
        a.seller,
        a.mint,
        A._inserted_timestamp,
        sum(b.amount) as sales_amount,
        FROM
        decoded A
        inner JOIN transfers b
        ON A.tx_id = b.tx_id
        and A.buyer_escrow_vault = b.tx_from
        and (a.seller = b.tx_to or a.tensor_sol_escrow = b.tx_to)
        AND A.index = b.index_1
        where a.event_type_temp in ('buyTensorSingleListingV1','buyTensorPoolListingV1')
        group by 1,2,3,4,5,6,7,8,9
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