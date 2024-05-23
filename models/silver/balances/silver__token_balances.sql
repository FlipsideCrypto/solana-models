{{ config(
    materialized = 'incremental',
    unique_key = ['token_balances_id'],
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH pre AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        account_index,
        account,
        mint,
        owner,
        DECIMAL,
        CASE
            WHEN amount = 0 THEN 0
            ELSE uiamount :: INT
        END AS pre_token_amount,
        _inserted_timestamp
    FROM
        {{ ref('silver___pre_token_balances') }}

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 31319460)+1,175418104)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 31319460)+8000000,175418104)
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
    -- block_id between 31319460 and 32319460
    block_id = 240134635
{% endif %}
),
post AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        account_index,
        account,
        mint,
        owner,
        DECIMAL,
        CASE
            WHEN amount = 0 THEN 0
            ELSE uiamount :: INT
        END AS post_token_amount,
        _inserted_timestamp
    FROM
        {{ ref('silver___post_token_balances') }}

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 31319460)+1,175418104)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 31319460)+8000000,175418104)
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
    -- block_id between 31319460 and 32319460
    block_id = 240134635
{% endif %}
),
pre_final as (
SELECT
    coalesce(a.block_timestamp,b.block_timestamp) block_timestamp,
    coalesce(a.block_id,b.block_id) block_id,
    coalesce(a.tx_id,b.tx_id) tx_id,
    coalesce(a.index,b.index) index,
    coalesce(a.account_index,b.account_index) account_index,
    coalesce(a.account,b.account) account,
    coalesce(a.mint,b.mint) mint,
    a.owner pre_owner,
    b.owner post_owner,
    a.pre_token_amount,
    b.post_token_amount,
    COALESCE(A._inserted_timestamp,b._inserted_timestamp) AS _inserted_timestamp
FROM
    pre A 
    FULL OUTER JOIN post b
    ON A.account_index = b.account_index
    AND A.tx_id = b.tx_id
)

SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    account_index,
    account,
    mint,
     pre_owner,
     post_owner,
    pre_token_amount,
    post_token_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','account']) }} as token_balances_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final
