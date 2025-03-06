-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['swaps_intermediate_meteora_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core','scheduled_non_core_hourly']
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.swaps_intermediate_meteora__intermediate_tmp AS 
        WITH distinct_entities AS (
            SELECT DISTINCT
                tx_id,
                block_timestamp
            FROM 
                {{ ref('silver__decoded_instructions_combined') }} d
            WHERE
                program_id IN (
                'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo', -- DLMM program
                'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' -- AMM program
                )
                AND event_type = 'swap'                
                AND succeeded
                {% if is_incremental() %}
                AND _inserted_timestamp >= (
                    SELECT
                        MAX(_inserted_timestamp) - INTERVAL '1 hour'
                    FROM
                        {{ this }}
                )
                {% endif %}
        )
        /* need to re-select all decoded instructions from all tx_ids in incremental subset 
        in order for the window function to output accurate values */
        SELECT 
            d.block_timestamp,
            d.block_id,
            d.tx_id,
            d.index,
            d.inner_index,
            d.signers,
            d.succeeded,
            d.program_id,
            d.decoded_instruction,
            d.event_type,
            d._inserted_timestamp
        FROM 
            {{ ref('silver__decoded_instructions_combined') }} d
        JOIN
            distinct_entities
            USING(tx_id)
        WHERE
             program_id IN (
            'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
            'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB'
        )
        AND event_type = 'swap'                
        AND succeeded
        AND block_timestamp >= (
            SELECT
                MIN(block_timestamp)
            FROM
                distinct_entities
        )
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.swaps_intermediate_meteora__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.swaps_intermediate_meteora__intermediate_tmp
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
        silver.udf_get_account_pubkey_by_name(
            'user',
            decoded_instruction :accounts
        ) AS swapper,
        silver.udf_get_account_pubkey_by_name(
            'userTokenIn',
            decoded_instruction :accounts
        ) AS source_token_account,
        silver.udf_get_account_pubkey_by_name(
            'tokenXMint',
            decoded_instruction :accounts
        ) AS source_mint,
        silver.udf_get_account_pubkey_by_name(
            'tokenYMint',
            decoded_instruction :accounts
        ) AS destination_mint,
        silver.udf_get_account_pubkey_by_name(
            'userTokenOut',
            decoded_instruction :accounts
        ) AS destination_token_account,
        silver.udf_get_account_pubkey_by_name(
            'reserveY',
            decoded_instruction :accounts
        ) AS program_destination_token_account,
        silver.udf_get_account_pubkey_by_name(
            'reserveX',
            decoded_instruction :accounts
        ) AS program_source_token_account,
        _inserted_timestamp
    FROM
        base
    WHERE
        program_id = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'
    UNION ALL
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        COALESCE(LEAD(inner_index) OVER (PARTITION BY tx_id, index 
            ORDER BY
            inner_index) -1, 999999
        ) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name(
            'user',
            decoded_instruction :accounts
        ) AS swapper,
        silver.udf_get_account_pubkey_by_name(
            'userSourceToken',
            decoded_instruction :accounts
        ) AS source_token_account,
        NULL AS source_mint,
        NULL AS destination_mint,
        silver.udf_get_account_pubkey_by_name(
            'userDestinationToken',
            decoded_instruction :accounts
        ) AS destination_token_account,
        silver.udf_get_account_pubkey_by_name(
            'aTokenVault',
            decoded_instruction :accounts
        ) AS program_destination_token_account,
        silver.udf_get_account_pubkey_by_name(
            'bTokenVault',
            decoded_instruction :accounts
        ) AS program_source_token_account,
        _inserted_timestamp
    FROM
        base
    WHERE
        program_id = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB'
        AND (
            decoded_instruction :args :amountIn :: INT <> 0
            OR decoded_instruction :args :inAmount :: INT <> 0
        )
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
                DISTINCT 
                    tx_id,
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
        COALESCE (
            b.succeeded,
            d.succeeded
        ) AS succeeded,
        A.swapper,
        COALESCE (
            b.amount,
            d.amount
        ) AS from_amt,
        COALESCE(
            b.mint,
            d.mint
        ) AS from_mint,
        COALESCE(
            C.amount,
            e.amount
        ) AS to_amt,
        COALESCE(
            C.mint,
            e.mint
        ) AS to_mint,
        A._inserted_timestamp
    FROM
        decoded A
        LEFT JOIN transfers b
        ON A.tx_id = b.tx_id
        AND A.source_token_account = b.source_token_account
        AND A.program_source_token_account = b.dest_token_account
        AND A.index = b.index_1
        AND (
            (
                b.inner_index_1 BETWEEN A.inner_index
                AND A.inner_index_end
            )
            OR A.inner_index IS NULL
        )
        LEFT JOIN transfers C
        ON A.tx_id = C.tx_id
        AND A.destination_token_account = C.dest_token_account
        AND A.program_destination_token_account = C.source_token_account
        AND A.index = C.index_1
        AND (
            (
                C.inner_index_1 BETWEEN A.inner_index
                AND A.inner_index_end
            )
            OR A.inner_index IS NULL
        )
        LEFT JOIN transfers d
        ON A.tx_id = d.tx_id
        AND A.source_token_account = d.source_token_account
        AND A.program_destination_token_account = d.dest_token_account
        AND A.index = d.index_1
        AND (
            (
                d.inner_index_1 BETWEEN A.inner_index
                AND A.inner_index_end
            )
            OR A.inner_index IS NULL
        )
        LEFT JOIN transfers e
        ON A.tx_id = e.tx_id
        AND A.destination_token_account = e.dest_token_account
        AND A.program_source_token_account = e.source_token_account
        AND A.index = e.index_1
        AND (
            (
                e.inner_index_1 BETWEEN A.inner_index
                AND A.inner_index_end
            )
            OR A.inner_index IS NULL
        ) 
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
    /* 
        handle edge cases where the transaction is depositing a small amount of some token and there is no corresponding WSOL/SOL transfer back to the owner
       when this is done via Jupiter v6, it is listed as to_amount/to_mint as 0/WSOL
       we will default to this pattern for native meteora swaps as well that meet this criteria
    */
    CASE
        WHEN to_amt IS NULL AND from_mint IS NOT NULL and from_amt IS NOT NULL AND program_id = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' THEN
            0
        ELSE
            to_amt
    END AS to_amt,
    CASE
        WHEN to_mint IS NULL AND from_mint IS NOT NULL AND from_amt IS NOT NULL AND program_id = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' THEN
            'So11111111111111111111111111111111111111112'
        ELSE
            to_mint
    END AS to_mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','swap_index','program_id']) }} AS swaps_intermediate_meteora_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final