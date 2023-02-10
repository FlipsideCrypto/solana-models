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
                    COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
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
                    COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
                ),
                CURRENT_DATE - 1
            )
        FROM
            {{ this }}
    ) 
{% elif is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    _inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
{% endif %}
),
tokens_in AS (
    SELECT
        block_timestamp :: DATE AS b_date,
        purchaser AS signer,
        mint AS token_in,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_mints') }}
    WHERE
        block_timestamp :: DATE >= CURRENT_DATE - 7
        AND block_timestamp :: DATE IN (
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
                COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
) {% elif not is_incremental() %}
AND _inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
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

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND t._inserted_timestamp < (
    SELECT
        LEAST(
            DATEADD(
                'day',
                2,
                COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
) 
AND e._inserted_timestamp < (
    SELECT
        LEAST(
            DATEADD(
                'day',
                2,
                COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
) 
{% elif not is_incremental() %}
AND t._inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
AND e._inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
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
    event_type = 'initializeAccount3'
    AND block_timestamp :: DATE >= CURRENT_DATE - 7
            AND block_timestamp :: DATE IN (
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
                    COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
                ),
                CURRENT_DATE - 1
            )
        FROM
            {{ this }}
    ) {% elif not is_incremental() %}
    AND _inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
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
                COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
) {% elif not is_incremental() %}
AND _inserted_timestamp :: DATE BETWEEN '2023-01-15'
AND '2023-02-06'
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

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND t._inserted_timestamp < (
    SELECT
        LEAST(
            DATEADD(
                'day',
                2,
                COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
)
AND e._inserted_timestamp < (
    SELECT
        LEAST(
            DATEADD(
                'day',
                2,
                COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
)  
{% elif not is_incremental() %}
AND t._inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
AND e._inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
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
                COALESCE(MAX(_inserted_timestamp :: DATE), '2023-01-15')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
) {% elif not is_incremental() %}
AND _inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
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
