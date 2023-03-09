{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', b_date, signer, token_in, _inserted_timestamp)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'b_date'
) }}

SELECT
    block_timestamp :: DATE AS b_date,
    COALESCE(
        owner,
        purchaser
    ) AS signer,
    mint AS token_in,
    m._inserted_timestamp
FROM
    {{ ref('silver__nft_mints') }} m
    LEFT OUTER JOIN {{ ref('silver__token_account_owners') }} o
    ON m.token_account = o.account_address
WHERE
    end_block_id IS NULL 
    AND succeeded

{% if is_incremental() %}
AND
    m._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
AND
    o._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    AND m._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
    AND o._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
{% endif %}

UNION

SELECT
    block_timestamp :: DATE AS b_date,
    tx_to AS signer,
    t.mint AS token_in,
    t._inserted_timestamp
FROM
    {{ ref('silver__transfers') }}
    t
    INNER JOIN {{ ref('silver___nft_distinct_mints') }}
    e
    ON e.mint = t.mint
WHERE 
    succeeded
{% if is_incremental() %}
AND
    t._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
AND
    e._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    AND t._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
    AND e._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
{% endif %}

UNION

SELECT
    block_timestamp :: DATE AS b_date,
    owner AS signer,
    o.mint AS token_in,
    o._inserted_timestamp
FROM
    {{ ref('silver__token_account_ownership_events') }}
    o
    INNER JOIN {{ ref('silver___nft_distinct_mints') }}
    e
    ON e.mint = o.mint
WHERE
    event_type ilike 'initializeAccount%' 
    AND succeeded

{% if is_incremental() %}
AND
    o._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    AND o._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
{% endif %}

UNION

SELECT
    block_timestamp :: DATE AS b_date,
    purchaser AS signer,
    mint AS token_in,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_magic_eden_v2') }}
WHERE 
    succeeded
{% if is_incremental() %}
    AND _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        )
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
{% endif %}
