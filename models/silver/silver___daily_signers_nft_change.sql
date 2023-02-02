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
    _inserted_timestamp :: DATE BETWEEN '2022-08-12'
    AND '2022-08-30'
{% endif %}
),
include_mints AS (
    SELECT
        DISTINCT mint
    FROM
        {{ ref('core__fact_nft_mints') }}
    UNION
    SELECT
        DISTINCT mint
    FROM
        {{ ref('core__fact_nft_sales') }}
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
        {# purchaser IN (
            '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H',
            'Hsg1qaafF8FUoqrhRPTtmEX86Hsv3Tc8t33iE8tNqUSb')  #}
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
                COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
) {% elif not is_incremental() %}
AND _inserted_timestamp :: DATE BETWEEN '2022-08-12'
AND '2022-08-30'
{% endif %}


UNION
SELECT
    block_timestamp :: DATE AS b_date,
    purchaser AS signer,
    mint AS token_in,
    NULL AS _inserted_timestamp
FROM
    {{ ref('core__fact_nft_sales') }}
{# WHERE
    purchaser IN (
        '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H',
        'Hsg1qaafF8FUoqrhRPTtmEX86Hsv3Tc8t33iE8tNqUSb'
    ) #}
UNION
SELECT
    block_timestamp :: DATE AS b_date,
    tx_to AS signer,
    t.mint AS token_in,
    _inserted_timestamp
FROM
    {{ ref('silver__transfers') }}
    t
    INNER JOIN include_mints e
    ON e.mint = t.mint
WHERE
    {# tx_to IN (
        '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H',
        'Hsg1qaafF8FUoqrhRPTtmEX86Hsv3Tc8t33iE8tNqUSb')  #}
        e.block_timestamp :: DATE >= CURRENT_DATE - 7
    AND e.block_timestamp :: DATE IN (
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
) {% elif not is_incremental() %}
AND e._inserted_timestamp :: DATE BETWEEN '2022-08-12'
AND '2022-08-30'
{% endif %}


),
tokens_out AS (
    SELECT
        block_timestamp :: DATE AS b_date,
        burner AS signer,
        mint AS token_out,
        _inserted_timestamp
    FROM
        {{ ref('silver___nft_burns') }}
    WHERE
        {# burner IN (
            '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H',
            'Hsg1qaafF8FUoqrhRPTtmEX86Hsv3Tc8t33iE8tNqUSb')  #}
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
                COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
) {% elif not is_incremental() %}
AND _inserted_timestamp :: DATE BETWEEN '2022-08-12'
AND '2022-08-30'
{% endif %}


UNION
SELECT
    block_timestamp :: DATE AS b_date,
    seller AS signer,
    mint AS token_out,
    NULL AS _inserted_timestamp
FROM
    {{ ref('core__fact_nft_sales') }}
{# WHERE
    seller IN (
        '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H',
        'Hsg1qaafF8FUoqrhRPTtmEX86Hsv3Tc8t33iE8tNqUSb'
    ) #}
UNION
SELECT
    block_timestamp :: DATE AS b_date,
    tx_from AS signer,
    t.mint AS token_out,
    _inserted_timestamp
FROM
    {{ ref('silver__transfers') }}
    t
    INNER JOIN include_mints e
    ON e.mint = t.mint
WHERE
    {# tx_from IN (
        '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H',
        'Hsg1qaafF8FUoqrhRPTtmEX86Hsv3Tc8t33iE8tNqUSb')  #}
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
                COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
            ),
            CURRENT_DATE - 1
        )
    FROM
        {{ this }}
) {% elif not is_incremental() %}
AND _inserted_timestamp :: DATE BETWEEN '2022-08-12'
AND '2022-08-30'
{% endif %}


), 
ins AS (
    SELECT
        b_date,
        signer,
        ARRAY_AGG(token_in) AS nfts_in
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
        ARRAY_AGG(token_out) AS nfts_out
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
    nfts_out
FROM
    ins i 
    FULL OUTER JOIN outs o
    ON i.signer = o.signer
    AND i.b_date = o.b_date
