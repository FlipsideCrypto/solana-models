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
    AND block_timestamp :: DATE >= '2023-01-07'
{% endif %}
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        _inserted_timestamp,
        program_id,
        silver.udf_get_account_pubkey_by_name('user', decoded_instruction:accounts) as swapper,
        silver.udf_get_account_pubkey_by_name('userTokenIn', decoded_instruction:accounts) as source_token_account, --1 source
        silver.udf_get_account_pubkey_by_name('tokenXMint', decoded_instruction:accounts) as source_mint,
        silver.udf_get_account_pubkey_by_name('tokenYMint', decoded_instruction:accounts) as destination_mint,
        silver.udf_get_account_pubkey_by_name('userTokenOut', decoded_instruction:accounts) as destination_token_account, --2 dest
        silver.udf_get_account_pubkey_by_name('reserveY', decoded_instruction:accounts) as program_destination_token_account, --2 source
        silver.udf_get_account_pubkey_by_name('reserveX', decoded_instruction:accounts) as program_source_token_account -- 1 dest
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
        _inserted_timestamp,
        program_id,
        silver.udf_get_account_pubkey_by_name('user', decoded_instruction:accounts) as swapper,
        silver.udf_get_account_pubkey_by_name('userSourceToken', decoded_instruction:accounts) as source_token_account,
        null as source_mint,
        null as destination_mint,
        silver.udf_get_account_pubkey_by_name('userDestinationToken', decoded_instruction:accounts) as destination_token_account,
        silver.udf_get_account_pubkey_by_name('aTokenVault', decoded_instruction:accounts) as program_destination_token_account,
        silver.udf_get_account_pubkey_by_name('bTokenVault', decoded_instruction:accounts) as program_source_token_account 
    FROM
        base
    where program_id = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB'
    and (decoded_instruction:args:amountIn::int <> 0 or decoded_instruction:args:inAmount::int <> 0)
),
transfers AS (
    SELECT
        A.*
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
    AND A.block_timestamp :: DATE >= '2023-01-07'
{% endif %}
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.program_id,
    A.tx_id,
    ROW_NUMBER() over (
        PARTITION BY a.tx_id
        ORDER BY
            a.INDEX,
            a.inner_index
    ) AS swap_index,
    coalesce (b.succeeded,d.succeeded) AS succeeded,
    A.swapper,
    coalesce (b.amount,d.amount) AS from_amt,
    coalesce(b.mint,d.mint) AS from_mint,
    coalesce(C.amount,e.amount) AS to_amt,
    coalesce(c.mint,e.mint) AS to_mint,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['A.tx_id','swap_index','A.program_id']) }} AS swaps_intermediate_meteora_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    decoded A
    left JOIN transfers b
    ON A.tx_id = b.tx_id
    AND A.source_token_account = b.source_token_account
    AND A.program_source_token_account = b.dest_token_account
    AND COALESCE(SPLIT_PART(b.index :: text, '.', 1) :: INT, b.index :: INT) = A.index
    left JOIN transfers C
    ON A.tx_id = C.tx_id
    AND A.destination_token_account = C.dest_token_account
    AND A.program_destination_token_account = C.source_token_account
    AND COALESCE(SPLIT_PART(C.index :: text, '.', 1) :: INT, C.index :: INT) = A.index
    left JOIN transfers d
    ON A.tx_id = d.tx_id
    AND A.source_token_account = d.source_token_account
    AND A.program_destination_token_account = d.dest_token_account
    AND COALESCE(SPLIT_PART(d.index :: text, '.', 1) :: INT, d.index :: INT) = A.index
    left JOIN transfers e
    ON A.tx_id = e.tx_id
    AND A.destination_token_account = e.dest_token_account
    AND A.program_source_token_account = e.source_token_account
    AND COALESCE(SPLIT_PART(e.index :: text, '.', 1) :: INT, e.index :: INT) = A.index
