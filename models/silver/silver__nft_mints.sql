{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base_mint_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__mint_actions') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
base_mint_price AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_mint_price') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
initialization AS (
    SELECT
        *
    FROM
        base_mint_actions
    WHERE
        event_type IN (
            'initializeMint',
            'initializeMint2'
        )
        AND succeeded
),
first_mint AS (
    SELECT
        *
    FROM
        base_mint_actions
    WHERE
        event_type NOT IN (
            'initializeMint',
            'initializeMint2'
        )
        AND succeeded qualify(ROW_NUMBER() over (PARTITION BY mint
    ORDER BY
        block_timestamp)) = 1
),
pre_final AS (
    SELECT
        i.block_id,
        i.block_timestamp,
        i.tx_id,
        i.succeeded,
        i.mint,
        i.decimal,
        f.mint_amount
    FROM
        initialization i
        LEFT OUTER JOIN first_mint f
        ON i.mint = f.mint
),
b AS (
    SELECT
        *,
        CASE
            WHEN DECIMAL = 0
            AND mint_amount = 1 THEN 'nft'
            WHEN DECIMAL <> 0
            OR mint_amount > 1 THEN 'token'
            ELSE 'unknown'
        END AS mint_type
    FROM
        pre_final
)
SELECT
    b.block_id,
    b.block_timestamp,
    b.succeeded,
    b.tx_id,
    b.mint,
    mp.payer,
    mp.mint_price
FROM
    b
    LEFT OUTER JOIN base_mint_price mp
    ON b.mint = mp.mint
WHERE
    b.mint_type IN (
        'nft',
        'unknown'
    )
