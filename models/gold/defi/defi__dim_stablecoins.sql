-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'token_address',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['daily']
) }}



WITH crosschain_stablecoins AS (
SELECT
    s.token_address,
    UPPER(COALESCE(s.symbol, m.symbol)) AS symbol,
    COALESCE(
        s.name,
        m.name
    ) AS NAME,
    m.decimals,
    m.is_verified,
    m.is_verified_modified_timestamp
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
WHERE
    m.is_verified --verified stablecoins only
    and s.blockchain = 'solana'

{% if is_incremental() %}
AND s.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)

SELECT 
    token_address,
    symbol,
    name,
    CONCAT(symbol,': ',name) AS label,
    decimals,
    is_verified,
    is_verified_modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address']) }} AS dim_stablecoins_id
FROM
    crosschain_stablecoins