{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', b_date, signer, token_in, _inserted_timestamp)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'b_date'
) }}

WITH dates_changed AS (

    SELECT
        DISTINCT block_timestamp :: DATE AS block_timestamp_date
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        succeeded

{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic(
        this,
        30,
        '2023-02-16'
    ) }}
{% endif %}
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05' 
{% endif %}
)
SELECT
    block_timestamp :: DATE AS b_date,
    COALESCE(
        owner,
        purchaser
    ) AS signer,
    mint AS token_in,
    m._inserted_timestamp
FROM
    {{ ref('silver__nft_mints') }}
    m
    LEFT OUTER JOIN {{ ref('silver__token_account_owners') }}
    o
    ON m.token_account = o.account_address
WHERE
    end_block_id IS NULL 
    AND block_timestamp :: DATE >= CURRENT_DATE - 10 
    AND block_timestamp :: DATE IN (
        SELECT
            block_timestamp_date
        FROM
            dates_changed
    )

{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic_with_alias(
        this,
        30,
        '2023-02-16',
        'm'
    ) }}
    {{ get_batch_load_logic_with_alias(
        this,
        30,
        '2023-02-16',
        'o'
    ) }}
{% endif %}
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
    t.block_timestamp :: DATE >= CURRENT_DATE - 10  
    AND t.block_timestamp :: DATE IN (
        SELECT
            block_timestamp_date
        FROM
            dates_changed
    )

{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic_with_alias(
        this,
        30,
        '2023-02-16',
        't'
    ) }}
    {{ get_batch_load_logic_with_alias(
        this,
        30,
        '2023-02-16',
        'e'
    ) }}
{% endif %}
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
    event_type = 'initializeAccount3' 
    AND block_timestamp :: DATE >= CURRENT_DATE - 10
    AND block_timestamp :: DATE IN (
        SELECT
            block_timestamp_date
        FROM
            dates_changed
    )

{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic_with_alias(
        this,
        30,
        '2023-02-16',
        'e'
    ) }}
    {{ get_batch_load_logic_with_alias(
        this,
        30,
        '2023-02-16',
        'o'
    ) }}
{% endif %}
{% else %}
    AND e._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
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
    block_timestamp :: DATE >= CURRENT_DATE - 10
    AND block_timestamp :: DATE IN (
        SELECT
            block_timestamp_date
        FROM
            dates_changed
    )

{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic(
        this,
        30,
        '2023-02-16'
    ) }}
{% endif %}
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
{% endif %}
