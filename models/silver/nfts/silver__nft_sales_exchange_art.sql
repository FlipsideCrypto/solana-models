{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH buys AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        program_id,
        SUM(
            COALESCE(
                i.value :parsed :info :lamports :: NUMBER,
                0
            )
        ) / pow(
            10,
            9
        ) AS sales_amount,
        instruction :accounts [0] :: STRING AS purchaser,
        instruction :accounts [3] :: STRING AS seller,
        instruction :accounts [6] :: STRING AS mint,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        program_id = 'AmK5g2XcyptVLCFESBCJqoSfwV3znGoVYQnqEnaAZKWn' -- Exchange Art Buys
        AND NOT ARRAY_CONTAINS(
            'EXBuYPNgBUXMTsjCbezENRUtFQzjUNZxvPGTd11Pznk5' :: variant,
            instruction :accounts
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-10-30'
{% endif %}
GROUP BY
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    instruction :accounts [0] :: STRING,
    instruction :accounts [3] :: STRING,
    instruction :accounts [6] :: STRING,
    _inserted_timestamp
),
buy_nows AS (
    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        e.succeeded,
        program_id,
        SUM(
            COALESCE(
                i.value :parsed :info :lamports :: NUMBER,
                0
            )
        ) / pow(
            10,
            9
        ) AS sales_amount,
        instruction :accounts [0] :: STRING AS purchaser,
        instruction :accounts [1] :: STRING AS seller,
        instruction :accounts [3] :: STRING AS mint,
        e._inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON e.tx_id = t.tx_id
        LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
    WHERE
        e.program_id = 'EXBuYPNgBUXMTsjCbezENRUtFQzjUNZxvPGTd11Pznk5' -- Exchange Art Buy Nows
        AND l.value :: STRING = 'Program log: Instruction: ExecuteBuynowSale'
        AND e.succeeded = TRUE
        AND t.succeeded = TRUE

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
AND t._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND e.block_timestamp :: DATE >= '2021-10-30'
    AND t.block_timestamp :: DATE >= '2021-10-30'
{% endif %}
GROUP BY
    e.block_timestamp,
    e.block_id,
    e.tx_id,
    e.succeeded,
    program_id,
    instruction :accounts [0] :: STRING,
    instruction :accounts [1] :: STRING,
    instruction :accounts [3] :: STRING,
    e._inserted_timestamp
HAVING
    sales_amount <> 0
),
redeems AS (
    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        e.succeeded,
        program_id,
        instruction :accounts [3] :: STRING AS seller,
        instruction :accounts [6] :: STRING AS mint,
        instruction :accounts [4] :: STRING AS acct_1,
        e._inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON e.tx_id = t.tx_id
        LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
    WHERE
        program_id = 'exAuvFHqXXbiLrM4ce9m1icwuSyXytRnfBkajukDFuB'
        AND l.value :: STRING = 'Program log: processing AuctionInstruction::Redeem'

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
AND t._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND e.block_timestamp :: DATE >= '2021-10-30'
    AND t.block_timestamp :: DATE >= '2021-10-30'
{% endif %}
),
bid_txs AS (
    SELECT
        e.tx_id,
        instruction :accounts [0] :: STRING AS purchaser,
        instruction :accounts [2] :: STRING AS acct_1,
        i.value :parsed :info :lamports / pow(
            10,
            9
        ) AS bid_amount
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON e.tx_id = t.tx_id
        LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
        LEFT JOIN TABLE(FLATTEN(t.instructions)) i
    WHERE
        program_id = 'exAuvFHqXXbiLrM4ce9m1icwuSyXytRnfBkajukDFuB'
        AND l.value :: STRING ILIKE 'Program log: processing AuctionInstruction::RegisterBid%'
        AND i.value :parsed :info :lamports IS NOT NULL
        AND e.succeeded = TRUE
        AND t.succeeded = TRUE

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
AND t._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND e.block_timestamp :: DATE >= '2021-10-30'
    AND t.block_timestamp :: DATE >= '2021-10-30'
{% endif %}
),
final_bid AS (
    SELECT
        acct_1,
        MAX(bid_amount) AS sales_amount
    FROM
        bid_txs
    GROUP BY
        acct_1
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    sales_amount,
    purchaser,
    seller,
    mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','mint']
    ) }} AS nft_sales_exchange_art_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    buys
WHERE
    sales_amount > 0 -- removes transfers
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    sales_amount,
    purchaser,
    seller,
    mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','mint']
    ) }} AS nft_sales_exchange_art_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    buy_nows
UNION ALL
SELECT
    block_timestamp,
    block_id,
    r.tx_id,
    succeeded,
    program_id,
    bid_amount AS sales_amount,
    purchaser,
    seller,
    mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['r.tx_id','mint']
    ) }} AS nft_sales_exchange_art_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    redeems r
    INNER JOIN final_bid f
    ON r.acct_1 = f.acct_1
    INNER JOIN bid_txs b
    ON f.acct_1 = b.acct_1
    AND f.sales_amount = b.bid_amount
