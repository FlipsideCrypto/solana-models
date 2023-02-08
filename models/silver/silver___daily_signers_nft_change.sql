{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', signer, b_date)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'signer'
) }} 
{# WITH dates_changed AS (
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
), #}
WITH tokens_in AS (
    SELECT
        block_timestamp :: DATE AS b_date,
        purchaser AS signer,
        mint AS token_in,
        _inserted_timestamp
    FROM
        {{ ref('silver__nft_mints') }}
    WHERE
        purchaser = '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H'
        {# block_timestamp :: DATE >= CURRENT_DATE - 7
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
{% endif %} #}


UNION

SELECT
    block_timestamp :: DATE AS b_date,
    COALESCE(
        owner, 
        tx_to
    ) AS signer,
    t.mint AS token_in,
    t._inserted_timestamp
FROM
    {{ ref('silver__transfers') }}
    t
    INNER JOIN {{ ref('silver___nft_distinct_mints') }} e
    ON e.mint = t.mint
    FULL OUTER JOIN {{ ref('silver.account_owners') }} a
    ON t.source_token_account = a.account_address
WHERE
    COALESCE(owner, tx_to) = '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H'
    {# t.block_timestamp :: DATE >= CURRENT_DATE - 7
    AND t.block_timestamp :: DATE IN (
        SELECT
            block_timestamp_date
        FROM
            dates_changed
    )
    AND end_block_id IS NULL 
    AND token_in IS NOT NULL

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
AND a._inserted_timestamp < (
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
AND a._inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
{% endif %} #}
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
        burn_authority = '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H'
        {# block_timestamp :: DATE >= CURRENT_DATE - 7
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
{% endif %} #}

UNION

SELECT
    block_timestamp :: DATE AS b_date,
    COALESCE(
        owner, 
        tx_from
     ) AS signer,
    t.mint AS token_out,
    t._inserted_timestamp
FROM
    {{ ref('silver__transfers') }}
    t
    INNER JOIN {{ ref('silver___nft_distinct_mints') }} e
    ON e.mint = t.mint
    FULL OUTER JOIN {{ ref('silver__account_owners') }} a
    ON t.source_token_account = a.account_address
WHERE
    COALESCE(owner, tx_from) = '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H'
    {# block_timestamp :: DATE >= CURRENT_DATE - 7
    AND block_timestamp :: DATE IN (
        SELECT
            block_timestamp_date
        FROM
            dates_changed
    )
    AND end_block_id IS NOT NULL 
    AND token_out IS NOT NULL 

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
AND a._inserted_timestamp < (
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
AND a._inserted_timestamp :: DATE BETWEEN '2023-01-15' AND '2023-02-06'
{% endif %} #}

), 
ins AS (
    SELECT
        b_date,
        signer,
        ARRAY_AGG(token_in) AS nfts_in, 
        _inserted_timestamp
    FROM
        tokens_in
    GROUP BY
        b_date,
        signer, 
        _inserted_timestamp
),
outs AS (
    SELECT
        b_date,
        signer,
        ARRAY_AGG(token_out) AS nfts_out, 
        _inserted_timestamp
    FROM
        tokens_out
    GROUP BY
        b_date,
        signer, 
        _inserted_timestamp
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
