{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","_log_id"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base AS (

    SELECT
        *
    FROM
        solana.silver.decoded_instructions
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
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.index,
        A._inserted_timestamp,
        A.decoded_instruction :args :inAmount :: INT AS in_amount,
        MAX(
            CASE
                WHEN b.value :name = 'userTransferAuthority' THEN b.value :pubkey :: STRING
            END
        ) AS swapper,
        MAX(
            CASE
                WHEN b.value :name = 'sourceTokenAccount' THEN b.value :pubkey :: STRING
            END
        ) AS source_token_account,
        MAX(
            CASE
                WHEN b.value :name = 'sourceMint' THEN b.value :pubkey :: STRING
            END
        ) AS source_mint,
        MAX(
            CASE
                WHEN b.value :name = 'destinationMint' THEN b.value :pubkey :: STRING
            END
        ) AS destination_mint,
        MAX(
            CASE
                WHEN b.value :name = 'destinationTokenAccount' THEN b.value :pubkey :: STRING
            END
        ) AS destination_token_account,
        MAX(
            CASE
                WHEN b.value :name = 'programDestinationTokenAccount' THEN b.value :pubkey :: STRING
            END
        ) AS program_destination_token_account,
        MAX(
            CASE
                WHEN b.value :name = 'programSourceTokenAccount' THEN b.value :pubkey :: STRING
            END
        ) AS program_source_token_account
    FROM
        base A,
        LATERAL FLATTEN(
            input => A.decoded_instruction :accounts
        ) b
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6
),
transfers AS (
    SELECT
        A.*
    FROM
        solana.silver.transfers A
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                decoded
        ) d
        ON d.tx_id = A.tx_id

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    A.block_timestamp :: DATE >= '2023-07-04'
{% endif %}
)
SELECT
    A.block_id,
    A.block_timestamp,
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4' AS program_id,
    A.tx_id,
    C.succeeded,
    A.swapper,
    b.amount AS from_amt,
    A.source_mint AS from_mint,
    C.amount AS to_amt,
    A.destination_mint AS to_mint,
    A._inserted_timestamp,
    CONCAT(
        A.tx_id,
        '-',
        A.index
    ) AS _log_id
FROM
    decoded A
    LEFT JOIN transfers b
    ON A.tx_id = b.tx_id
    AND A.source_token_account = b.source_token_account
    AND A.program_source_token_account = b.dest_token_account
    AND COALESCE(SPLIT_PART(b.index :: text, '.', 1) :: INT, b.index :: INT) = A.index
    LEFT JOIN transfers C
    ON A.tx_id = C.tx_id
    AND A.destination_token_account = C.dest_token_account
    AND A.program_destination_token_account = C.source_token_account
    AND COALESCE(SPLIT_PART(C.index :: text, '.', 1) :: INT, C.index :: INT) = A.index
