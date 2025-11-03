-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'token_address',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['scheduled_non_core']
) }}

-- todo: add is_verified to asset_metadata tables


WITH crosschain_stablecoins AS (
SELECT
    s.token_address,
    UPPER(COALESCE(s.symbol, m.symbol)) AS symbol,
    COALESCE(
        s.name,
        m.name
    ) AS NAME,
    m.decimals,
    -- m.is_verified,
    -- m.is_verified_modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['s.token_address']) }} AS dim_stablecoins_id
FROM
    {{ source(
        'crosschain_silver',
        'tokens_stablecoins'
    ) }}
    s
    INNER JOIN {{ ref('price__ez_asset_metadata') }}
    m
    ON s.token_address = m.token_address
    AND s.blockchain = m.blockchain
-- WHERE
--     m.is_verified --verified stablecoins only

{% if is_incremental() %}
AND s.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),

-- manual_stablecoins AS (
-- SELECT
--     s.contract_address,
--     UPPER(
--         m.symbol
--     ) AS symbol,
--     m.name,
--     m.decimals,
--     m.is_verified,
--     m.is_verified_modified_timestamp,
--     SYSDATE() AS inserted_timestamp,
--     SYSDATE() AS modified_timestamp,
--     id placeholder
-- FROM
--     seed placeholder
--     s
--     INNER JOIN {{ ref('price__ez_asset_metadata') }}
--     m
--     ON s.contract_address = m.token_address
--     AND s.blockchain = m.blockchain
-- WHERE
--     m.is_verified --verified stablecoins only
-- {% if is_incremental() %}
-- AND s.contract_address NOT IN (
--     SELECT
--         contract_address
--     FROM
--         {{ this }}
-- )
-- {% endif %}
-- ),
all_stablecoins AS (
    SELECT * FROM crosschain_stablecoins
    -- UNION ALL
    -- SELECT * FROM manual_stablecoins
)
SELECT 
    token_address,
    symbol,
    name,
    decimals,
    -- is_verified,
    -- is_verified_modified_timestamp,
    inserted_timestamp,
    modified_timestamp,
    dim_stablecoins_id
FROM
    all_stablecoins