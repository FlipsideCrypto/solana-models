-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['swaps_intermediate_lifinity_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core','scheduled_non_core_hourly'],
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.swaps_intermediate_lifinity__intermediate_tmp AS
    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = '2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c'
        AND event_type = 'swap'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '1 hour'
        FROM
            {{ this }}
    )
    {% else %}
        AND _inserted_timestamp::DATE >= '2022-10-01'
    {% endif %}
    {% endset %}
    
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate(
        "silver.swaps_intermediate_lifinity__intermediate_tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.swaps_intermediate_lifinity__intermediate_tmp
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        program_id,
        succeeded,
        silver.udf_get_account_pubkey_by_name('userTransferAuthority', decoded_instruction:accounts) AS user_authority,
        silver.udf_get_account_pubkey_by_name('sourceInfo', decoded_instruction:accounts) AS source_token_account,
        silver.udf_get_account_pubkey_by_name('destinationInfo', decoded_instruction:accounts) AS destination_token_account,
        silver.udf_get_account_pubkey_by_name('swapSource', decoded_instruction:accounts) AS swap_source,
        silver.udf_get_account_pubkey_by_name('swapDestination', decoded_instruction:accounts) AS swap_destination,
        TRY_PARSE_JSON(decoded_instruction:args:amountIn)::number AS amount_in,
        TRY_PARSE_JSON(decoded_instruction:args:minimumAmountOut)::number AS min_amount_out,
        signers,
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
                DISTINCT tx_id,
                block_timestamp::DATE AS block_date
            FROM
                decoded
        ) d
        ON d.block_date = A.block_timestamp::DATE
        AND d.tx_id = A.tx_id
    WHERE
        {{ between_stmts }}
),
final_swaps AS (
    SELECT
        d.block_id,
        d.block_timestamp,
        d.program_id,
        d.tx_id,
        d.index,
        d.inner_index,
        d.succeeded,
        COALESCE(d.user_authority, d.signers[0]::STRING) AS swapper,
        from_t.amount AS from_amt,
        from_t.mint AS from_mint,
        to_t.amount AS to_amt,
        to_t.mint AS to_mint,
        d.amount_in,
        d.min_amount_out,
        d._inserted_timestamp
    FROM
        decoded d
        LEFT JOIN transfers from_t
        ON d.tx_id = from_t.tx_id
        AND d.source_token_account = from_t.source_token_account
        AND d.swap_source = from_t.dest_token_account
        AND d.index = from_t.index_1
        AND from_t.succeeded = TRUE
        LEFT JOIN transfers to_t
        ON d.tx_id = to_t.tx_id
        AND d.destination_token_account = to_t.dest_token_account
        AND d.swap_destination = to_t.source_token_account
        AND d.index = to_t.index_1
        AND to_t.succeeded = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.tx_id, d.index, d.inner_index ORDER BY COALESCE(from_t.amount, 0) DESC) = 1
)
SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    index,
    inner_index,
    ROW_NUMBER() OVER (
        PARTITION BY tx_id
        ORDER BY
            index,
            inner_index
    ) AS swap_index,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint,
    amount_in,
    min_amount_out,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index']) }} AS swaps_intermediate_lifinity_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final_swaps