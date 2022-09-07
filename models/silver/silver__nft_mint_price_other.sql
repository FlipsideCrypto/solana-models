{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', mint, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

WITH base_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE succeeded
{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
base_ptb AS (
    SELECT
        DISTINCT mint AS mint_paid,
        account,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
--unknown program...
hweq_fallback AS (
    SELECT
        e.block_timestamp,
        e.tx_id,
        e.index,
        i.index as inner_index,
        e.program_id,
        instruction :accounts [2] :: STRING AS mint,
        instruction :accounts [0] :: STRING AS payer,
        COALESCE(
            i.value :parsed :info :lamports :: INTEGER,
            i.value :parsed :info :amount :: INTEGER
        ) AS transfer_amount,
        CASE 
            WHEN i.value :parsed :info :lamports is not null THEN NULL 
            ELSE i.value :parsed :info :source
        END as token_account,
        e._inserted_timestamp
    FROM
        base_events e
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        program_id = 'HWeQ1ntizxmbMwVHemf9zncf2h6RTTfLiuzbjD9wAN9e'
        AND ARRAY_SIZE(
            instruction :accounts
        ) > 7
    group by 1,2,3,4,5,6,7,8,9,10
),
--unknown program...
multi_mints_fallback AS (
    SELECT
        e.block_timestamp,
        e.tx_id,
        e.index,
        i.index as inner_index,
        e.program_id,
        instruction :accounts [17] :: STRING AS mint,
        instruction :accounts [2] :: STRING AS payer,
        COALESCE(
            i.value :parsed :info :lamports :: INTEGER,
            i.value :parsed :info :amount :: INTEGER
        ) AS transfer_amount,
        CASE 
            WHEN i.value :parsed :info :lamports is not null THEN NULL 
            ELSE i.value :parsed :info :source
        END as token_account,
        e._inserted_timestamp
    FROM
        base_events e
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        program_id = '5WTCguyGQDrFosVn8M9JynwdoRpQJUPuzaembMwug35r'
        AND instruction :data :: STRING LIKE '2z8AjPpeqe%'
        AND mint is not null
        AND inner_index < 17
    group by 1,2,3,4,5,6,7,8,9,10
    UNION
    SELECT
        e.block_timestamp,
        e.tx_id,
        e.index,
        i.index as inner_index,
        e.program_id,
        instruction :accounts [21] :: STRING AS mint,
        instruction :accounts [2] :: STRING AS payer,
        COALESCE(
            i.value :parsed :info :lamports :: INTEGER,
            i.value :parsed :info :amount :: INTEGER
        ) AS transfer_amount,
        CASE 
            WHEN i.value :parsed :info :lamports is not null THEN NULL 
            ELSE i.value :parsed :info :source
        END as token_account,
        e._inserted_timestamp
    FROM
        base_events e
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        program_id = '5WTCguyGQDrFosVn8M9JynwdoRpQJUPuzaembMwug35r'
        AND instruction :data :: STRING LIKE '2z8AjPpeqe%'
        AND mint is not null
        AND inner_index >= 17
    group by 1,2,3,4,5,6,7,8,9,10
),
hweq_pre_final as (
    select 
        e.*,
        COALESCE(
            p.mint_paid,
            'So11111111111111111111111111111111111111111'
        ) AS mint_currency,
        COALESCE(p.decimal, 9) as decimal
    from hweq_fallback e
    LEFT OUTER JOIN base_ptb p on e.token_account = p.account
),
multi_mints_pre_final as (
    select 
        e.*,
        COALESCE(
            p.mint_paid,
            'So11111111111111111111111111111111111111111'
        ) AS mint_currency,
        COALESCE(p.decimal, 9) as decimal
    from multi_mints_fallback e
    LEFT OUTER JOIN base_ptb p on e.token_account = p.account
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
    hweq_pre_final p
GROUP BY 
    1, 
    2, 
    3, 
    4, 
    5
UNION 
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
    multi_mints_pre_final p
GROUP BY 
    1, 
    2, 
    3, 
    4, 
    5

-- SELECT
--     p.block_id,
--     p.block_timestamp,
--     p.tx_id,
--     p.succeeded,
--     p.program_id,
--     p.payer,
--     p.mint,
--     COALESCE(
--         m.mint_paid,
--         'So11111111111111111111111111111111111111111'
--     ) AS mint_currency,
--     p.mint_price / pow(10, COALESCE(m.decimal, 9)) AS mint_price,
--     _inserted_timestamp
-- FROM
--     hweq_fallback p
--     LEFT OUTER JOIN base_ptb m
--     ON p.tx_id = m.tx_id
--     AND p.payer = m.account
-- UNION
-- SELECT
--     p.block_id,
--     p.block_timestamp,
--     p.tx_id,
--     p.succeeded,
--     p.program_id,
--     p.payer,
--     p.mint,
--     COALESCE(
--         m.mint_paid,
--         'So11111111111111111111111111111111111111111'
--     ) AS mint_currency,
--     p.mint_price / pow(10, COALESCE(m.decimal, 9)) AS mint_price,
--     _inserted_timestamp
-- FROM
--     multi_mints_fallback p
--     LEFT OUTER JOIN base_ptb m
--     ON p.tx_id = m.tx_id
--     AND p.payer = m.account
