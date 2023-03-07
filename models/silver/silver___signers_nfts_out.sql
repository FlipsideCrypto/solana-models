{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', signer, token_out)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'signer'
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
    burn_authority AS signer,
    mint AS token_out,
    _inserted_timestamp
FROM
    {{ ref('silver__burn_actions') }}
WHERE
    block_timestamp :: DATE >= CURRENT_DATE - 10  
    AND succeeded
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
    block_timestamp :: DATE >= CURRENT_DATE - 10 
    AND succeeded
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
        'e'
    ) }}
    {{ get_batch_load_logic_with_alias(
        this,
        30,
        '2023-02-16',
        't'
    ) }}
{% endif %}
{% else %}
    AND e._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
    AND t._inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-05'
{% endif %}
UNION

SELECT
    block_timestamp :: DATE AS b_date,
    owner AS signer,
    mint AS token_out,
    _inserted_timestamp
FROM
    {{ ref('silver__token_account_ownership_events') }}
WHERE
    event_type = 'closeAccount'
    AND succeeded 
    AND block_timestamp :: DATE >= CURRENT_DATE - 10
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

UNION

SELECT
    block_timestamp :: DATE AS b_date,
    seller AS signer,
    mint AS token_out,
    _inserted_timestamp
FROM
    {{ ref('silver__nft_sales_magic_eden_v2') }}
WHERE
    block_timestamp :: DATE >= CURRENT_DATE - 10
    AND succeeded
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
