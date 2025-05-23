{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    unique_key = ['nft_sales_solsniper_id'],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    full_refresh = false,
    enabled = false,
) }}
-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set max_inserted_query %}

SELECT
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query).columns [0].values() [0] %}
{% endif %}

{% set base_query %}
CREATE OR REPLACE temporary TABLE silver.decoded_instructions_solsniper__intermediate_tmp AS
SELECT
    block_timestamp,
    block_id,
    tx_id,
    INDEX,
    inner_index,
    program_id,
    decoded_instruction,
    event_type,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_instructions_combined') }}
WHERE
    program_id = 'SNPRohhBurQwrpwAptw1QYtpFdfEKitr4WSJ125cN1g'
    AND event_type = 'executeSolNftOrder'

{% if is_incremental() %}
AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% else %}
    AND block_timestamp :: DATE >= '2023-05-02'
{% endif %}

{% endset %}
{% do run_query(
    base_query
) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate("silver.decoded_instructions_solsniper__intermediate_tmp","block_timestamp::date"
) %}
{% endif %}

WITH decoded AS (
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
        silver.decoded_instructions_solsniper__intermediate_tmp
),
transfers AS (
    SELECT
        A.*,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 1) :: INT, INDEX :: INT) AS index_1,
        NULLIF(SPLIT_PART(INDEX :: text, '.', 2), '') :: INT AS inner_index_1
    FROM
        {{ ref('silver__transfers') }} A
    INNER JOIN 
        (SELECT DISTINCT tx_id, block_timestamp::date as bt FROM decoded) d
        ON d.tx_id = A.tx_id
        AND d.bt = A.block_timestamp::date
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
        A.buyer AS purchaser,
        A.seller,
        C.amount AS fee_amt,
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
        AND A.treasury_fee_account = C.tx_to
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
        10,
        11,
        12
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
    sale_amt + fee_amt AS sales_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','mint']) }} AS nft_sales_solsniper_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final

