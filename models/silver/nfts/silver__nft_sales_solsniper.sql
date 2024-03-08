{{ config(
    materialized = 'incremental',
    unique_key = ['nft_sales_solsniper_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'SNPRohhBurQwrpwAptw1QYtpFdfEKitr4WSJ125cN1g'
        AND event_type = 'executeSolNftOrder'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-05-02'
{% endif %}
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        program_id,
        silver.udf_get_account_pubkey_by_name('buyer',decoded_instruction :accounts) AS buyer,
        silver.udf_get_account_pubkey_by_name('seller',decoded_instruction :accounts) AS seller, -- main payment sent here
        silver.udf_get_account_pubkey_by_name('treasury',decoded_instruction :accounts) AS treasury_fee_account, --fees sent here
        silver.udf_get_account_pubkey_by_name('buyerEscrowVault',decoded_instruction :accounts) AS buyer_escrow_vault, -- where payment comes from
        silver.udf_get_account_pubkey_by_name('sellNftMint',decoded_instruction :accounts) AS mint,
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
                DISTINCT tx_id
            FROM
                decoded
        ) d
        ON d.tx_id = A.tx_id
    WHERE
        A.succeeded

{% if is_incremental() %}
AND A._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 day'
    FROM
        {{ this }}
)
{% else %}
    AND A.block_timestamp :: DATE >= '2023-05-02'
{% endif %}
),
pre_final AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.program_id,
        A.tx_id,
        A.index,
        A.inner_index,
        b.succeeded,
        A.buyer AS purchaser,
        A.seller,
        b.amount AS sale_amt,
        C.amount AS fee_amt,
        A.mint,
        A._inserted_timestamp
    FROM
        decoded A
        INNER JOIN transfers b
        ON A.tx_id = b.tx_id
        AND A.buyer_escrow_vault = b.tx_from
        AND A.seller = b.tx_to
        AND A.index = b.index_1
        INNER JOIN transfers C
        ON A.tx_id = C.tx_id
        AND A.buyer_escrow_vault = C.tx_from
        AND A.treasury_fee_account = C.tx_to
        AND A.index = C.index_1
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
    {{ dbt_utils.generate_surrogate_key(['tx_id','mint']) }} AS nft_sales_solsniper_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final
