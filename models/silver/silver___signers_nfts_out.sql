{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', signer, token_out)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'signer'
) }}

SELECT
    block_timestamp :: DATE AS b_date,
    burn_authority AS signer,
    mint AS token_out,
    _inserted_timestamp
FROM
    {{ ref('silver__burn_actions') }}
WHERE
    succeeded

{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
{% endif %}

UNION

SELECT
    t.block_timestamp :: DATE AS b_date,
    tx_from AS signer,
    t.mint AS token_out,
    t._inserted_timestamp
FROM
    {{ ref('silver__transfers') }}
    t
    INNER JOIN {{ ref('silver___nft_distinct_mints') }}
    e
    ON e.mint = t.mint
WHERE
    succeeded
    AND (tx_from IS NOT NULL 
    AND t.mint IS NOT NULL) 

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
    o.mint AS token_out,
    o._inserted_timestamp
FROM
    {{ ref('silver__token_account_ownership_events') }} o
    INNER JOIN {{ ref('silver___nft_distinct_mints') }} e
    ON e.mint = o.mint
WHERE
    succeeded
    AND o.mint IS NOT NULL

{% if is_incremental() %}
AND
    o._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    e._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    AND o._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
    AND e._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
{% endif %}

UNION

SELECT
    block_timestamp :: DATE AS b_date,
    seller AS signer,
    mint AS token_out,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_magic_eden_v2') }}
WHERE
    succeeded

{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
{% endif %}
