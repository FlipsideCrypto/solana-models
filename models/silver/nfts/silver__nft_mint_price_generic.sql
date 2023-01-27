{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', mint, payer, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

WITH base_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE succeeded

-- new incremental logic
{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2021-06-02')),'2023-01-23')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2021-06-02')),'2023-01-23')
            FROM
                {{ this }}
        )
{% elif is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
AND 
    block_timestamp :: DATE BETWEEN '2021-06-02'
    AND '2021-06-17'

{% endif %}
),
base_ptb AS (
    SELECT
        distinct mint AS mint_paid,
        account,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }}

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2021-06-02')),'2023-01-23')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2021-06-02')),'2023-01-23')
            FROM
                {{ this }}
        ) 
{% elif is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
WHERE
    block_timestamp :: DATE BETWEEN '2021-06-02'
    AND '2021-06-17'
{% endif %}
),
metaplex_events AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        e.succeeded,
        e.program_id,
        e.index,
        e.instruction :accounts AS accounts,
        ARRAY_SIZE(accounts) AS num_accounts,
        e.inner_instruction,
        _inserted_timestamp
    FROM
        base_events e,
        TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        AND succeeded
        AND (
            (ARRAY_SIZE(accounts) = 6
            AND accounts [5] = '11111111111111111111111111111111')
            OR (ARRAY_SIZE(accounts) = 7
            AND accounts [5] = '11111111111111111111111111111111'
            AND accounts [6] = 'SysvarRent111111111111111111111111111111111')
            OR (ARRAY_SIZE(accounts) = 9
            AND accounts [7] = '11111111111111111111111111111111'
            AND accounts [8] = 'SysvarRent111111111111111111111111111111111')
            OR (ARRAY_SIZE(accounts) = 14
            AND accounts [11] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [12] = '11111111111111111111111111111111'
            AND accounts [13] = 'SysvarRent111111111111111111111111111111111')
            OR (ARRAY_SIZE(accounts) = 17
            AND accounts [13] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [14] = 'vau1zxA2LbssAUEF7Gpw91zMM1LvXrvpzJtmZ58rPsn'
            AND accounts [15] = '11111111111111111111111111111111'
            AND accounts [16] = 'SysvarRent111111111111111111111111111111111'))
    UNION
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        e.succeeded,
        e.program_id,
        e.index,
        i.value :accounts AS accounts,
        ARRAY_SIZE(accounts) AS num_accounts,
        e.inner_instruction,
        _inserted_timestamp
    FROM
        base_events e,
        TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        i.value :programId :: STRING = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        AND succeeded
        AND (
            (ARRAY_SIZE(accounts) = 6
            AND accounts [5] = '11111111111111111111111111111111')
            OR (ARRAY_SIZE(accounts) = 7
            AND accounts [5] = '11111111111111111111111111111111'
            AND accounts [6] = 'SysvarRent111111111111111111111111111111111')
            OR (ARRAY_SIZE(accounts) = 9
            AND accounts [7] = '11111111111111111111111111111111'
            AND accounts [8] = 'SysvarRent111111111111111111111111111111111')
            OR (ARRAY_SIZE(accounts) = 14
            AND accounts [11] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [12] = '11111111111111111111111111111111'
            AND accounts [13] = 'SysvarRent111111111111111111111111111111111')
            OR (ARRAY_SIZE(accounts) = 17
            AND accounts [13] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [14] = 'vau1zxA2LbssAUEF7Gpw91zMM1LvXrvpzJtmZ58rPsn'
            AND accounts [15] = '11111111111111111111111111111111'
            AND accounts [16] = 'SysvarRent111111111111111111111111111111111'))
),
mint_price_events AS (
    SELECT
        me.block_timestamp,
        me.tx_id,
        me.index,
        i.index as inner_index,
        'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' as program_id,
        i.value:parsed:info:destination::string as temp_destination,
        i.value:parsed:info:source::string as temp_source,
        CASE
            WHEN num_accounts in (14,17) THEN me.accounts [3] :: STRING
            ELSE me.accounts [1] :: STRING
        END AS mint,
        CASE
            WHEN num_accounts in (14,17) THEN me.accounts [6] :: STRING
            WHEN num_accounts = 9 THEN me.accounts [4] :: STRING
            ELSE me.accounts [3] :: STRING
        END AS payer,
        COALESCE(
            i.value :parsed :info :lamports :: INTEGER,
            i.value :parsed :info :amount :: INTEGER
        ) AS transfer_amount,
        CASE 
            WHEN i.value :parsed :info :lamports is not null THEN NULL 
            ELSE i.value :parsed :info :source
        END as token_account,
        me._inserted_timestamp
    FROM
        metaplex_events me
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    group by 1,2,3,4,5,6,7,8,9,10,11,12
),
pre_final as (
    select 
        e.*,
        COALESCE(
            p.mint_paid,
            'So11111111111111111111111111111111111111111'
        ) AS mint_currency,
        COALESCE(p.decimal, 9) as decimal
    from mint_price_events e
    LEFT OUTER JOIN base_ptb p on e.token_account = p.account
    where (temp_destination <> temp_source) or (temp_destination is null) or (temp_source is null)
)
SELECT
    p.mint,
    p.payer,
    mint_currency,
    decimal,
    p.program_id,
    SUM(p.transfer_amount / pow(10, decimal)) AS mint_price,
    array_unique_agg(tx_id) as tx_ids,
    max(block_timestamp) as block_timestamp,
    max(_inserted_timestamp) as _inserted_timestamp
FROM
    pre_final p
GROUP BY 
    1, 
    2, 
    3, 
    4, 
    5
