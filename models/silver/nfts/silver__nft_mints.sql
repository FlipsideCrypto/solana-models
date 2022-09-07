{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH b AS (

    SELECT
        tx_id
    FROM
        {{ ref('silver__events') }}
        e
    WHERE
        event_type IN (
            'mintTo',
            'initializeMint'
        )
        AND succeeded

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
EXCEPT
SELECT
    tx_id
FROM
    {{ ref('silver__nft_mints_tmp') }}
    m

{% if is_incremental() %}
WHERE m._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    mint_currency,
    mint,
    mint_price,
    ingested_at,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_mints_tmp') }}
    e
{% if is_incremental() %}
WHERE e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION
SELECT
    e.block_timestamp,
    e.block_id,
    e.tx_id,
    e.succeeded,
    e.program_id,
    COALESCE(
        instruction :parsed :info :mintAuthority :: STRING,
        instruction :parsed :info :multisigMintAuthority :: STRING
    ) AS purchaser,
    NULL AS mint_currency,
    instruction :parsed :info :mint :: STRING AS mint,
    NULL AS mint_price,
    e.ingested_at,
    e._inserted_timestamp
FROM
    {{ ref('silver__events') }}
    e
    INNER JOIN b
    ON b.tx_id = e.tx_id
WHERE
    event_type IN (
        'mintTo',
        'initializeMint'
    )

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11
