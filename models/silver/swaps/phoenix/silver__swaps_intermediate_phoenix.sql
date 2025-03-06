 -- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['swaps_intermediate_phoenix_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core_hourly'],
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.swaps_intermediate_phoenix__intermediate_tmp AS 
        SELECT 
            *
        FROM 
            {{ ref('silver__decoded_instructions_combined') }}
        WHERE
            program_id = 'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY'
        AND event_type = 'Swap'
        AND succeeded
        {% if is_incremental() %}
        AND _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) - INTERVAL '1 hour'
            FROM
                {{ this }}
        )
        {% else %} 
            AND _inserted_timestamp :: DATE >= '2024-02-14'
            AND _inserted_timestamp :: DATE < '2024-02-17'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.swaps_intermediate_phoenix__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (

    SELECT
        *
    FROM
        silver.swaps_intermediate_phoenix__intermediate_tmp
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        COALESCE(inner_index,-1) AS inner_index_start,
        COALESCE(LEAD(inner_index) over (PARTITION BY tx_id, INDEX
    ORDER BY
        inner_index) -1, 999999) AS inner_index_end,
        program_id,
        CASE
            WHEN decoded_instruction :args :orderPacket :immediateOrCancel :side :bid IS NOT NULL THEN 'bid'
            ELSE 'ask'
        END AS side,
        silver.udf_get_account_pubkey_by_name(
            'trader',
            decoded_instruction :accounts
        ) AS swapper,
        CASE
            WHEN side = 'ask' THEN silver.udf_get_account_pubkey_by_name(
                'quoteVault',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'baseVault',
                decoded_instruction :accounts
            )
        END AS source_token_account,
        NULL AS source_mint,
        NULL AS destination_mint,
        CASE
            WHEN side = 'ask' THEN silver.udf_get_account_pubkey_by_name(
                'baseVault',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'quoteVault',
                decoded_instruction :accounts
            )
        END AS destination_token_account,
        CASE
            WHEN side = 'ask' THEN silver.udf_get_account_pubkey_by_name(
                'baseAccount',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'quoteAccount',
                decoded_instruction :accounts
            )
        END AS program_destination_token_account,
        CASE
            WHEN side = 'ask' THEN silver.udf_get_account_pubkey_by_name(
                'quoteAccount',
                decoded_instruction :accounts
            )
            ELSE silver.udf_get_account_pubkey_by_name(
                'baseAccount',
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
    INNER JOIN 
    (SELECT DISTINCT tx_id, block_timestamp::date as bt FROM decoded) d
        ON d.tx_id = A.tx_id
        AND d.bt = A.block_timestamp::date
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
        c.amount AS from_amt,
        c.mint AS from_mint,
        b.amount AS to_amt,
        b.mint AS to_mint,
        A._inserted_timestamp
    FROM
        decoded A
        INNER JOIN transfers b
        ON A.tx_id = b.tx_id
        AND A.source_token_account = b.source_token_account
        AND A.program_source_token_account = b.dest_token_account
        AND A.index = b.index_1
        AND b.inner_index_1 BETWEEN A.inner_index_start AND A.inner_index_end
        INNER JOIN transfers C
        ON A.tx_id = C.tx_id
        AND A.destination_token_account = C.dest_token_account
        AND A.program_destination_token_account = C.source_token_account
        AND A.index = C.index_1
        AND C.inner_index_1 BETWEEN A.inner_index_start  AND A.inner_index_end
        qualify(ROW_NUMBER() over (PARTITION BY A.tx_id, A.index, A.inner_INDEX
    ORDER BY
        inner_index)) = 1
)
SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    index,
    inner_index,
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
    {{ dbt_utils.generate_surrogate_key(['tx_id','swap_index','program_id']) }} AS swaps_intermediate_phoenix_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final
