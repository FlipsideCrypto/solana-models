-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = "swaps_intermediate_saber_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core','scheduled_non_core_hourly'],
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.swaps_intermediate_saber__intermediate_tmp AS
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
        program_id = 'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ'
        AND event_type = 'swap'
        AND succeeded

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '1 hour'
        FROM
            {{ this }}
    )
    {% else %}
        AND _inserted_timestamp::DATE between '2025-04-30' and '2025-05-03'
    {% endif %}
    {% endset %}
    
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate(
        "silver.swaps_intermediate_saber__intermediate_tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

with base AS (
    SELECT
        *
    FROM
        silver.swaps_intermediate_saber__intermediate_tmp
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        COALESCE(LEAD(inner_index) OVER (PARTITION BY tx_id, index
            ORDER BY inner_index) -1, 999999
        ) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name('userAuthority', decoded_instruction:accounts) as swapper,
        silver.udf_get_account_pubkey_by_name('inputUser', decoded_instruction:accounts) as source_token_account,
        null as source_mint,
        null as destination_mint,
        silver.udf_get_account_pubkey_by_name('outputUserTokenUser', decoded_instruction:accounts) as destination_token_account,  
        silver.udf_get_account_pubkey_by_name('outputUserTokenReserve', decoded_instruction:accounts) as program_destination_token_account, 
        silver.udf_get_account_pubkey_by_name('inputReserve', decoded_instruction:accounts) as program_source_token_account,
        _inserted_timestamp
    FROM
        base
)
,
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
        A.succeeded
        and {{ between_stmts }}
),
pre_final AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.program_id,
        A.tx_id,
        A.index,
        A.inner_index,
        A.inner_index_end,
        C.succeeded,
        A.swapper,
        b.amount AS from_amt,
        b.mint AS from_mint,
        C.amount AS to_amt,
        C.mint AS to_mint,
        A._inserted_timestamp
    FROM
        decoded A
        LEFT JOIN transfers b
        ON A.tx_id = b.tx_id
        AND A.source_token_account = b.source_token_account
        AND A.program_source_token_account = b.dest_token_account
        AND A.index = b.index_1
        AND (
            (b.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end)
            OR A.inner_index IS NULL
        )
        LEFT JOIN transfers C
        ON A.tx_id = C.tx_id
        AND A.destination_token_account = C.dest_token_account
        AND A.program_destination_token_account = C.source_token_account
        AND A.index = C.index_1
        AND (
            (C.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end)
            OR A.inner_index IS NULL
        ) 
    QUALIFY ROW_NUMBER() over (PARTITION BY A.tx_id, A.index, A.inner_INDEX ORDER BY inner_index) = 1
)
SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    succeeded,
    ROW_NUMBER() over (
        PARTITION BY tx_id
        ORDER BY
            INDEX,
            inner_index
    ) AS swap_index,
    index,
    inner_index,
    swapper,
    from_amt as swap_from_amount,
    from_mint as swap_from_mint,
    to_amt as swap_to_amount,
    to_mint as swap_to_mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index']) }} AS swaps_intermediate_saber_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
WHERE
    (swap_to_mint is not null and swap_to_amount is not null)