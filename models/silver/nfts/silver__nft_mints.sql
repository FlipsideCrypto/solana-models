{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', initialization_tx_id, mint, purchaser, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_non_core']
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
        f.token_account, 
        i.decimal,
        f.mint_amount,
        i._inserted_timestamp
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
    b.tx_id as initialization_tx_id,
    mp.program_id,
    mp.payer as purchaser,
    b.mint,
    b.token_account, 
    mp.mint_currency,
    mp.mint_price,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['initialization_tx_id', 'b.mint', 'purchaser', 'mp.mint_currency']
    ) }} AS nft_mints_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    b
    LEFT OUTER JOIN base_mint_price mp
    ON b.mint = mp.mint
WHERE
    b.mint_type IN (
        'nft',
        'unknown'
    )
