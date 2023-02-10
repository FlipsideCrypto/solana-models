{{ config(
    materialized = 'incremental',
    unique_key = "mint",
    incremental_strategy = 'delete+insert',
) }}

WITH base AS (

    SELECT
        mint,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_mints') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ source(
        'solana_silver',
        'nft_sales_magic_eden_v1'
    ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_magic_eden_v2') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_solanart') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_smb') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ source(
        'solana_silver',
        'nft_sales_solport'
    ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_opensea') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_yawww') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_hadeswap') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_hyperspace') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_coral_cube') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    mint,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_exchange_art') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    mint,
    _inserted_timestamp
FROM
    base 
WHERE mint is not null
    qualify(ROW_NUMBER() over (PARTITION BY mint
ORDER BY
    _inserted_timestamp DESC)) = 1
