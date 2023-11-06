{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','index','program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions') }}
    WHERE
        program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-07-04'
{% endif %}
),
decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        _inserted_timestamp,
        program_id,
        solana_dev.silver.udf_get_account_pubkey_by_name('userTransferAuthority', decoded_instruction:accounts) as swapper,
        solana_dev.silver.udf_get_account_pubkey_by_name('sourceTokenAccount', decoded_instruction:accounts) as source_token_account,
        solana_dev.silver.udf_get_account_pubkey_by_name('sourceMint', decoded_instruction:accounts) as source_mint,
        solana_dev.silver.udf_get_account_pubkey_by_name('destinationMint', decoded_instruction:accounts) as destination_mint,
        solana_dev.silver.udf_get_account_pubkey_by_name('destinationTokenAccount', decoded_instruction:accounts) as destination_token_account,
        solana_dev.silver.udf_get_account_pubkey_by_name('programDestinationTokenAccount', decoded_instruction:accounts) as program_destination_token_account,
        solana_dev.silver.udf_get_account_pubkey_by_name('programSourceTokenAccount', decoded_instruction:accounts) as program_source_token_account
    FROM
        base
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
        where a.succeeded

{% if is_incremental() %}
AND
    A._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
AND
    A.block_timestamp :: DATE >= '2023-07-04'
{% endif %}
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.program_id,
    A.tx_id,
    A.index,
    C.succeeded,
    A.swapper,
    b.amount AS from_amt,
    A.source_mint AS from_mint,
    C.amount AS to_amt,
    A.destination_mint AS to_mint,
    A._inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(['a.tx_id','a.index','a.program_id']) }} as swaps_intermediate_jupiterv6_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    decoded A
    inner JOIN transfers b
    ON A.tx_id = b.tx_id
    AND A.source_token_account = b.source_token_account
    AND A.program_source_token_account = b.dest_token_account
    AND COALESCE(SPLIT_PART(b.index :: text, '.', 1) :: INT, b.index :: INT) = A.index
    inner JOIN transfers C
    ON A.tx_id = C.tx_id
    AND A.destination_token_account = C.dest_token_account
    AND A.program_destination_token_account = C.source_token_account
    AND COALESCE(SPLIT_PART(C.index :: text, '.', 1) :: INT, C.index :: INT) = A.index
