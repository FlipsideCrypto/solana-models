{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', signer, b_date)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'signer',
    full_refresh = false
) }}

WITH dates_changed AS (
    SELECT
        DISTINCT block_timestamp :: DATE AS block_timestamp_date
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    _inserted_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(
                DATEADD(
                    'day',
                    1,
                    COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                ),
                CURRENT_DATE - 1
            )
        FROM
            {{ this }}
    )
    AND (
        SELECT
            LEAST(
                DATEADD(
                    'day',
                    1,
                    COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                ),
                CURRENT_DATE - 1
            )
        FROM
            {{ this }}
    ) {% elif is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-08-30'
{% endif %}
),
include_mints AS (
    SELECT 
        DISTINCT mint 
    FROM {{ ref('core__fact_nft_mints') }}

    UNION 

    SELECT 
        DISTINCT mint
    FROM {{ ref('core__fact_nft_sales')}}
),
tokens_in AS (
    SELECT 
        block_timestamp :: date as b_date,
        purchaser as signer, 
        mint as token_in, 
        _inserted_timestamp
    FROM {{ ref('silver__nft_mints')}}
    WHERE
        block_timestamp :: DATE >= current_date - 7
    AND
        block_timestamp :: DATE IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        )
    {% if is_incremental() and env_var(
        'DBT_IS_BATCH_LOAD',
        "false"
    ) == "true" %}
        AND _inserted_timestamp < (
            SELECT
                LEAST(
                    DATEADD(
                        'day',
                        2,
                        COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                    ),
                    CURRENT_DATE - 1
                )
            FROM
                {{ this }}
        )
    {% elif not is_incremental() %}
        AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-08-30'
    {% endif %}

    UNION 

    SELECT 
        block_timestamp :: date as b_date, 
        purchaser as signer, 
        mint as token_in, 
        NULL as _inserted_timestamp
    FROM {{ ref('core__fact_nft_sales') }}

    UNION 

    SELECT 
        block_timestamp :: date as b_date, 
        tx_to AS signer, 
        t.mint AS token_in, 
        _inserted_timestamp
    FROM {{ ref('silver__transfers')}} t
    INNER JOIN include_mints e
    ON e.mint = t.mint
    WHERE
        e.block_timestamp :: DATE >= current_date - 7
    AND
        e.block_timestamp :: DATE IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        )
    {% if is_incremental() and env_var(
        'DBT_IS_BATCH_LOAD',
        "false"
    ) == "true" %}
        AND e._inserted_timestamp < (
            SELECT
                LEAST(
                    DATEADD(
                        'day',
                        2,
                        COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                    ),
                    CURRENT_DATE - 1
                )
            FROM
                {{ this }}
        )
    {% elif not is_incremental() %}
        AND e._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-08-30'
    {% endif %}
), 
tokens_out AS (
    SELECT 
        block_timestamp :: date as b_date,  
        burner AS signer, 
        mint AS token_out, 
        _inserted_timestamp
    FROM {{ ref('silver___nft_burns') }}
    WHERE
        block_timestamp :: DATE >= current_date - 7
    AND
        block_timestamp :: DATE IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        )
    {% if is_incremental() and env_var(
        'DBT_IS_BATCH_LOAD',
        "false"
    ) == "true" %}
        AND _inserted_timestamp < (
            SELECT
                LEAST(
                    DATEADD(
                        'day',
                        2,
                        COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                    ),
                    CURRENT_DATE - 1
                )
            FROM
                {{ this }}
        )
    {% elif not is_incremental() %}
        AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-08-30'
    {% endif %}

    UNION 

    SELECT 
        block_timestamp :: date as b_date,
        seller as signer, 
        mint as token_out, 
        NULL as _inserted_timestamp
    FROM {{ ref('core__fact_nft_sales')}}

    UNION 

    SELECT 
        block_timestamp :: date as b_date, 
        tx_from as signer, 
        t.mint as token_out, 
        _inserted_timestamp 
    FROM {{ ref('silver__transfers') }} t
    INNER JOIN include_mints e
    ON e.mint = t.mint
    WHERE
        block_timestamp :: DATE >= current_date - 7
    AND
        block_timestamp :: DATE IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        )
    {% if is_incremental() and env_var(
        'DBT_IS_BATCH_LOAD',
        "false"
    ) == "true" %}
        AND _inserted_timestamp < (
            SELECT
                LEAST(
                    DATEADD(
                        'day',
                        2,
                        COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                    ),
                    CURRENT_DATE - 1
                )
            FROM
                {{ this }}
        )
    {% elif not is_incremental() %}
        AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-08-30'
    {% endif %}
)
SELECT 
    i.b_date,
    i.signer, 
    array_agg(token_in) as nfts_in, 
    array_agg(token_out) as nfts_out
FROM tokens_in i 
LEFT OUTER JOIN tokens_out o
ON i.signer = o.signer
AND i.b_date = o.b_date
GROUP BY i.b_date, i.signer

