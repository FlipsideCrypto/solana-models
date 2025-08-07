-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = "swaps_pancakeswap_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['scheduled_non_core','scheduled_non_core_hourly'],
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.swaps_pancakeswap__intermediate_tmp AS
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
        program_id = 'HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq'
        AND event_type in ('swap','swap_v2')
        AND succeeded

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '1 hour'
        FROM
            {{ this }}
    )
    {% else %}
        AND _inserted_timestamp::DATE >= '2025-08-05'
    {% endif %}
    {% endset %}
    
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate(
        "silver.swaps_pancakeswap__intermediate_tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

with base AS (
    SELECT
        *
    FROM
        silver.swaps_pancakeswap__intermediate_tmp
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        COALESCE(
            LEAD(inner_index) OVER (
                PARTITION BY tx_id, index
                ORDER BY inner_index
            ) - 1,
            999999
        ) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name('payer', decoded_instruction:accounts) AS swapper,
        silver.udf_get_account_pubkey_by_name('input_token_account', decoded_instruction:accounts) AS source_token_account, -- 1 txfer source
        NULL AS source_mint,
        NULL AS destination_mint,
        silver.udf_get_account_pubkey_by_name('output_token_account', decoded_instruction:accounts) AS destination_token_account,  -- 2 txfer dest
        silver.udf_get_account_pubkey_by_name('output_vault', decoded_instruction:accounts) AS program_destination_token_account,  -- 2 source
        silver.udf_get_account_pubkey_by_name('input_vault', decoded_instruction:accounts) AS program_source_token_account, -- 1 dest
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
        b.amount AS swap_from_amount,
        b.mint AS swap_from_mint,
        C.amount AS swap_to_amount,
        C.mint AS swap_to_mint,
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
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index']) }} AS swaps_pancakeswap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final