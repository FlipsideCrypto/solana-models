{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', signer, b_date)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'signer'
) }} 
WITH dates_changed AS (
    SELECT
        DISTINCT block_timestamp :: DATE AS block_timestamp_date
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,30,'2023-02-14') }}
        {% endif %}
    {% else %}
        AND _inserted_timestamp::date between '2023-02-05' and '2023-02-14'
    {% endif %}
),
tokens_in AS (
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
        AND block_timestamp :: DATE >= CURRENT_DATE - 7
        AND block_timestamp :: DATE IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        ) 

{% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,30,'2023-02-14') }}
        {% endif %}
    {% else %}
        AND m._inserted_timestamp::date between '2023-02-05' and '2023-02-14'
        AND o._inserted_timestamp::date between '2023-02-05' and '2023-02-14'
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
    INNER JOIN {{ ref('silver___nft_distinct_mints') }} e
    ON e.mint = t.mint
WHERE
    t.block_timestamp :: DATE >= CURRENT_DATE - 7
    AND t.block_timestamp :: DATE IN (
        SELECT
            block_timestamp_date
        FROM
            dates_changed
    )

{% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,30,'2023-02-14') }}
        {% endif %}
    {% else %}
        AND t._inserted_timestamp::date between '2023-02-05' and '2023-02-14'
        AND e._inserted_timestamp::date between '2023-02-05' and '2023-02-14'
    {% endif %}

UNION 

SELECT 
    block_timestamp :: DATE as b_date, 
    owner AS signer, 
    o.mint AS token_in, 
    o._inserted_timestamp
FROM 
    {{ ref('silver__token_account_ownership_events') }} o
    INNER JOIN {{ ref('silver___nft_distinct_mints') }} e
    ON e.mint = o.mint

WHERE 
    event_type = 'initializeAccount3'
    AND block_timestamp :: DATE >= CURRENT_DATE - 7
            AND block_timestamp :: DATE IN (
                SELECT
                    block_timestamp_date
                FROM
                    dates_changed
            ) 

    {% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,30,'2023-02-14') }}
        {% endif %}
    {% else %}
        AND o._inserted_timestamp::date between '2023-02-05' and '2023-02-14'
        AND e._inserted_timestamp::date between '2023-02-05' and '2023-02-14'
    {% endif %}
),
tokens_out AS (
    SELECT
        block_timestamp :: DATE AS b_date,
        burn_authority AS signer,
        mint AS token_out,
        _inserted_timestamp
    FROM
        {{ ref('silver__burn_actions') }}
    WHERE
        block_timestamp :: DATE >= CURRENT_DATE - 7
        AND block_timestamp :: DATE IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        )

{% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,30,'2023-02-14') }}
        {% endif %}
    {% else %}
        AND _inserted_timestamp::date between '2023-02-05' and '2023-02-14'
    {% endif %}

UNION

SELECT
    block_timestamp :: DATE AS b_date,
    tx_from AS signer,
    t.mint AS token_out,
    t._inserted_timestamp
FROM
    {{ ref('silver__transfers') }}
    t
    INNER JOIN {{ ref('silver___nft_distinct_mints') }} e
    ON e.mint = t.mint
WHERE
    block_timestamp :: DATE >= CURRENT_DATE - 7
    AND block_timestamp :: DATE IN (
        SELECT
            block_timestamp_date
        FROM
            dates_changed
    )

{% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,30,'2023-02-14') }}
        {% endif %}
    {% else %}
        AND t._inserted_timestamp::date between '2023-02-05' and '2023-02-14'
        AND e._inserted_timestamp::date between '2023-02-05' and '2023-02-14'
    {% endif %}
UNION 

SELECT 
    block_timestamp :: DATE as b_date, 
    owner AS signer, 
    mint AS token_in, 
    _inserted_timestamp
FROM 
    {{ ref('silver__token_account_ownership_events') }} 
WHERE 
    event_type = 'closeAccount'
    AND block_timestamp :: DATE >= CURRENT_DATE - 7
        AND block_timestamp :: DATE IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        ) 

{% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,30,'2023-02-14') }}
        {% endif %}
    {% else %}
        AND _inserted_timestamp::date between '2023-02-05' and '2023-02-14'
    {% endif %} 

UNION 

SELECT 
    block_timestamp :: date as b_date, 
    seller as signer, 
    mint as token_out, 
    _inserted_timestamp
FROM 
    {{ ref('silver__nft_sales_magic_eden_v2')}}
WHERE 
    block_timestamp :: DATE >= CURRENT_DATE - 7
        AND block_timestamp :: DATE IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        ) 

{% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,30,'2023-02-14') }}
        {% endif %}
    {% else %}
        AND _inserted_timestamp::date between '2023-02-05' and '2023-02-14'
    {% endif %} 

), 
ins AS (
    SELECT
        b_date,
        signer,
        ARRAY_AGG(token_in) AS nfts_in, 
        MAX(_inserted_timestamp) as _inserted_timestamp
    FROM
        tokens_in
    GROUP BY
        b_date,
        signer
),
outs AS (
    SELECT
        b_date,
        signer,
        ARRAY_AGG(token_out) AS nfts_out, 
        MAX(_inserted_timestamp) as _inserted_timestamp
    FROM
        tokens_out
    GROUP BY
        b_date,
        signer
)
SELECT
    COALESCE(
        i.b_date,
        o.b_date
    ) AS b_date,
    COALESCE(
        i.signer,
        o.signer
    ) AS signer,
    nfts_in,
    nfts_out, 
    i._inserted_timestamp
FROM
    ins i 
    FULL OUTER JOIN outs o
    ON i.signer = o.signer
    AND i.b_date = o.b_date
