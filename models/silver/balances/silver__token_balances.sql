{{ config(
    materialized = 'incremental',
    unique_key = ['token_balances_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    full_refresh = false,
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core','daily_balances']
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
            ELSE uiamount :: FLOAT
        END AS pre_token_amount,
        tx_index,
        _inserted_timestamp
    FROM
        {{ ref('silver___pre_token_balances') }}
    WHERE
        succeeded

{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic(this, 15, '2024-06-09') }}
{% endif %}
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-01'
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
            ELSE uiamount :: FLOAT
        END AS post_token_amount,
        tx_index,
        _inserted_timestamp
    FROM
        {{ ref('silver___post_token_balances') }}
    WHERE
        succeeded

{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic(this, 15, '2024-06-09') }}
{% endif %}
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-01'
{% endif %}
),
pre_final AS (
    SELECT
        COALESCE(
            A.block_timestamp,
            b.block_timestamp
        ) block_timestamp,
        COALESCE(
            A.block_id,
            b.block_id
        ) block_id,
        COALESCE(
            A.tx_id,
            b.tx_id
        ) tx_id,
        COALESCE(
            A.tx_index,
            b.tx_index
        ) tx_index,
        TRUE AS succeeded,
        COALESCE(
            A.index,
            b.index
        ) INDEX,
        COALESCE(
            A.account_index,
            b.account_index
        ) account_index,
        COALESCE(
            A.account,
            b.account
        ) account,
        COALESCE(
            A.mint,
            b.mint
        ) mint,
        A.owner pre_owner,
        b.owner post_owner,
        COALESCE(
            A.pre_token_amount,
            0
        ) AS pre_token_amount,
        COALESCE(
            b.post_token_amount,
            0
        ) AS post_token_amount,
        COALESCE(
            A._inserted_timestamp,
            b._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        pre A 
        FULL OUTER JOIN 
        post b
        ON A.account_index = b.account_index
        AND A.tx_id = b.tx_id
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    tx_index,
    succeeded,
    INDEX,
    account_index,
    account,
    mint,
    pre_owner,
    post_owner,
    pre_token_amount,
    post_token_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','account']) }} AS token_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
WHERE 
    pre_token_amount <> post_token_amount
