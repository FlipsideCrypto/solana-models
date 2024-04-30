-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['swaps_intermediate_bonkswap_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core'],
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.swaps_intermediate_bonkswap__intermediate_tmp AS
    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p'
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
        AND block_timestamp :: DATE BETWEEN '2023-04-12' AND '2024-01-01'
    {% endif %}
    {% endset %}
    
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate(
        "silver.swaps_intermediate_bonkswap__intermediate_tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.swaps_intermediate_bonkswap__intermediate_tmp
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        decoded_instruction :args :xToY :: BOOLEAN AS xToY,
        COALESCE(LEAD(inner_index) OVER (PARTITION BY tx_id, index
            ORDER BY inner_index) -1, 999999
        ) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name(
            'swapper',
            decoded_instruction :accounts
        ) AS swapper,
        CASE
            WHEN xToY THEN silver.udf_get_account_pubkey_by_name(
                'swapperXAccount',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'swapperYAccount',
                decoded_instruction :accounts
            )
        END AS source_token_account,
        silver.udf_get_account_pubkey_by_name(
            'tokenX',
            decoded_instruction :accounts
        ) AS source_mint,
        silver.udf_get_account_pubkey_by_name(
            'tokenY',
            decoded_instruction :accounts
        ) AS destination_mint,
        CASE
            WHEN xToY THEN silver.udf_get_account_pubkey_by_name(
                'swapperYAccount',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'swapperXAccount',
                decoded_instruction :accounts
            )
        END AS destination_token_account,
        CASE
            WHEN xToY THEN silver.udf_get_account_pubkey_by_name(
                'poolYAccount',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'poolXAccount',
                decoded_instruction :accounts
            )
        END AS program_destination_token_account,
        CASE
            WHEN xToY THEN silver.udf_get_account_pubkey_by_name(
                'poolXAccount',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'poolYAccount',
                decoded_instruction :accounts
            )
        END AS program_source_token_account,
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
        A.succeeded
        AND {{ between_stmts }}
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
        INNER JOIN transfers b
        ON A.tx_id = b.tx_id
        AND A.source_token_account = b.source_token_account
        AND A.program_source_token_account = b.dest_token_account
        AND A.index = b.index_1
        AND ((b.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end) or a.inner_index is null)
        INNER JOIN transfers C
        ON A.tx_id = C.tx_id
        AND A.destination_token_account = C.dest_token_account
        AND A.program_destination_token_account = C.source_token_account
        AND A.index = C.index_1
        AND ((C.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end) or a.inner_index is null)
        AND C.amount <> 0
    QUALIFY
        ROW_NUMBER() over (PARTITION BY A.tx_id, A.index, A.inner_INDEX
            ORDER BY inner_index
        ) = 1
)
SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    ROW_NUMBER() over (
        PARTITION BY tx_id
        ORDER BY
            INDEX,
            inner_index
    ) AS swap_index,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','swap_index','program_id']) }} AS swaps_intermediate_bonkswap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final
