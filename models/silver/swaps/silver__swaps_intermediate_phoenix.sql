{{ config(
    materialized = 'incremental',
    unique_key = ['swaps_intermediate_phoenix_id'],
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
        program_id = 'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY'
    AND 
        event_type = 'Swap'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-02-15'
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
        CASE
            WHEN decoded_instruction :args :orderPacket :immediateOrCancel :side :bid IS NOT NULL THEN 'bid'
            ELSE 'ask'
        END AS side,
        solana.silver.udf_get_account_pubkey_by_name(
            'trader',
            decoded_instruction :accounts
        ) AS swapper,
        CASE
            WHEN side = 'ask' THEN solana.silver.udf_get_account_pubkey_by_name(
                'quoteVault',
                decoded_instruction :accounts
            )
            ELSE solana.silver.udf_get_account_pubkey_by_name(
                'baseVault',
                decoded_instruction :accounts
            )
        END AS source_token_account,
        NULL AS source_mint,
        NULL AS destination_mint,
        CASE
            WHEN side = 'ask' THEN solana.silver.udf_get_account_pubkey_by_name(
                'baseVault',
                decoded_instruction :accounts
            )
            ELSE solana.silver.udf_get_account_pubkey_by_name(
                'quoteVault',
                decoded_instruction :accounts
            )
        END AS destination_token_account,
        CASE
            WHEN side = 'ask' THEN solana.silver.udf_get_account_pubkey_by_name(
                'baseAccount',
                decoded_instruction :accounts
            )
            ELSE solana.silver.udf_get_account_pubkey_by_name(
                'quoteAccount',
                decoded_instruction :accounts
            )
        END AS program_destination_token_account,
        CASE
            WHEN side = 'ask' THEN solana.silver.udf_get_account_pubkey_by_name(
                'quoteAccount',
                decoded_instruction :accounts
            )
            ELSE solana.silver.udf_get_account_pubkey_by_name(
                'baseAccount',
                decoded_instruction :accounts
            )
        END AS program_source_token_account
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
    AND A.block_timestamp :: DATE >= '2023-02-15'
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
    C.succeeded,
    A.swapper,
    b.amount AS from_amt,
    b.mint AS from_mint,
    C.amount AS to_amt,
    c.mint AS to_mint,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['A.tx_id','swap_index','A.program_id']) }} AS swaps_intermediate_phoenix_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    decoded A
    INNER JOIN transfers b
    ON A.tx_id = b.tx_id
    AND A.source_token_account = b.source_token_account
    AND A.program_source_token_account = b.dest_token_account
    AND COALESCE(SPLIT_PART(b.index :: text, '.', 1) :: INT, b.index :: INT) = A.index
    INNER JOIN transfers C
    ON A.tx_id = C.tx_id
    AND A.destination_token_account = C.dest_token_account
    AND A.program_destination_token_account = C.source_token_account
    AND COALESCE(SPLIT_PART(C.index :: text, '.', 1) :: INT, C.index :: INT) = A.index
