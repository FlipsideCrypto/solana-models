{{ config(
    materialized = 'incremental',
    unique_key = ['swaps_intermediate_meteora_id'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core'],
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id in ('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo', -- DLMM program
                       'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' -- AMM program
                        )
    AND 
        event_type = 'swap'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-07-14'
{% endif %}
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        COALESCE(LEAD(inner_index) over (PARTITION BY tx_id, INDEX
        ORDER BY
        inner_index) -1, 999999) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name('user', decoded_instruction:accounts) as swapper,
        silver.udf_get_account_pubkey_by_name('userTokenIn', decoded_instruction:accounts) as source_token_account,
        silver.udf_get_account_pubkey_by_name('tokenXMint', decoded_instruction:accounts) as source_mint,
        silver.udf_get_account_pubkey_by_name('tokenYMint', decoded_instruction:accounts) as destination_mint,
        silver.udf_get_account_pubkey_by_name('userTokenOut', decoded_instruction:accounts) as destination_token_account,
        silver.udf_get_account_pubkey_by_name('reserveY', decoded_instruction:accounts) as program_destination_token_account,
        silver.udf_get_account_pubkey_by_name('reserveX', decoded_instruction:accounts) as program_source_token_account,
        _inserted_timestamp
    FROM
        base
    where program_id = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'
    union all
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        COALESCE(LEAD(inner_index) over (PARTITION BY tx_id, INDEX
        ORDER BY
        inner_index) -1, 999999) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name('user', decoded_instruction:accounts) as swapper,
        silver.udf_get_account_pubkey_by_name('userSourceToken', decoded_instruction:accounts) as source_token_account,
        null as source_mint,
        null as destination_mint,
        silver.udf_get_account_pubkey_by_name('userDestinationToken', decoded_instruction:accounts) as destination_token_account,
        silver.udf_get_account_pubkey_by_name('aTokenVault', decoded_instruction:accounts) as program_destination_token_account,
        silver.udf_get_account_pubkey_by_name('bTokenVault', decoded_instruction:accounts) as program_source_token_account,
        _inserted_timestamp
    FROM
        base
    where program_id = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB'
    and (decoded_instruction:args:amountIn::int <> 0 or decoded_instruction:args:inAmount::int <> 0)
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
    AND A.block_timestamp :: DATE >= '2022-07-14'
{% endif %}
),
pre_final as (
SELECT
    A.block_id,
    A.block_timestamp,
    A.program_id,
    A.tx_id,
    A.index,
    A.inner_index,
    A.inner_index_end,
    coalesce (b.succeeded,d.succeeded) AS succeeded,
    A.swapper,
    coalesce (b.amount,d.amount) AS from_amt,
    coalesce(b.mint,d.mint) AS from_mint,
    coalesce(C.amount,e.amount) AS to_amt,
    coalesce(c.mint,e.mint) AS to_mint,
    A._inserted_timestamp
FROM
    decoded A
    left JOIN transfers b
    ON A.tx_id = b.tx_id
    AND A.source_token_account = b.source_token_account
    AND A.program_source_token_account = b.dest_token_account
    AND A.index = b.index_1
    AND ((b.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end) or a.inner_index is null)
    left JOIN transfers C
    ON A.tx_id = C.tx_id
    AND A.destination_token_account = C.dest_token_account
    AND A.program_destination_token_account = C.source_token_account
    AND A.index = C.index_1
    AND ((C.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end) or a.inner_index is null)
    left JOIN transfers d
    ON A.tx_id = d.tx_id
    AND A.source_token_account = d.source_token_account
    AND A.program_destination_token_account = d.dest_token_account
    AND A.index = d.index_1
    AND ((d.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end) or a.inner_index is null)
    left JOIN transfers e
    ON A.tx_id = e.tx_id
    AND A.destination_token_account = e.dest_token_account
    AND A.program_source_token_account = e.source_token_account
    AND A.index = e.index_1
    AND ((e.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end) or a.inner_index is null)
    qualify(ROW_NUMBER() over (PARTITION BY A.tx_id, A.index, A.inner_INDEX ORDER BY inner_index)) = 1)

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
    {{ dbt_utils.generate_surrogate_key(['tx_id','swap_index','program_id']) }} AS swaps_intermediate_meteora_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final
