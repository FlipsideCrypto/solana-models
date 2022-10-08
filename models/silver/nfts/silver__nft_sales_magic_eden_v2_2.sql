{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH txs AS (

    SELECT
        e.tx_id,
        t.succeeded,
        t.signers[0] :: STRING as signer, 
        MAX(INDEX) AS max_event_index
    FROM
        {{ ref('silver__events2') }}
        e
        INNER JOIN {{ ref('silver__transactions2') }}
        t
        ON t.tx_id = e.tx_id
    WHERE
        program_id = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' -- Magic Eden V2 Program ID
{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    t.block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
            FROM
                {{ this }}
        )
AND
    e.block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
            FROM
                {{ this }}
        ) 
{% elif is_incremental() %}
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
    AND 
        e.block_timestamp :: DATE BETWEEN '2022-01-08' -- no ME V2 contract before this date
        AND '2022-02-08'
    AND 
        t.block_timestamp :: DATE BETWEEN '2022-01-08' -- no ME V2 contract before this date
        AND '2022-02-08'
{% endif %}
GROUP BY
    1,
    2, 
    3
HAVING
    COUNT(
        e.tx_id
    ) >= 2
),
base_tmp AS (
    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        t.succeeded,
        e.index AS event_index,
        i.index AS inner_index,
        e.program_id,
        COALESCE(
            i.value :parsed :info :lamports :: NUMBER,
            0
        ) AS amount,
        instruction :accounts [7] :: STRING AS nft_account,
        instruction :accounts [0] :: STRING AS purchaser,
        i.value :parsed :type :: STRING AS inner_instruction_type,
        LAG(inner_instruction_type) over (
            PARTITION BY e.tx_id
            ORDER BY
                inner_index
        ) AS preceding_inner_instruction_type,
        -- some mints do not map to a token account because of post purchase transfers within same transaction...need to use this when it is available
        LAST_VALUE(
            i.value :parsed :info :mint :: STRING ignore nulls
        ) over (
            PARTITION BY e.tx_id
            ORDER BY
                inner_index
        ) AS nft_account_mint,
        _inserted_timestamp
    FROM
        {{ ref('silver__events2') }}
        e
        INNER JOIN txs t
        ON t.tx_id = e.tx_id
        AND t.max_event_index = e.index
        AND ARRAY_SIZE(
            e.inner_instruction :instructions
        ) > 1
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        (
            (
                amount <> 0
                AND inner_instruction_type = 'transfer'
            )
            OR inner_instruction_type = 'create'
        )
        AND array_size(e.instruction:accounts) > 12

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    e.block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
            FROM
                {{ this }}
        )
{% elif is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND 
        e.block_timestamp :: DATE BETWEEN '2022-01-08' -- no ME V2 contract before this date
        AND '2022-02-08'
{% endif %}
),
sellers AS (
     SELECT
        e.tx_id,
        instruction :accounts [1] :: STRING AS seller
    FROM
        {{ ref('silver__events2') }}
        e
        INNER JOIN txs t
        ON t.tx_id = e.tx_id
        AND t.max_event_index = e.index
        AND ARRAY_SIZE(
            e.inner_instruction :instructions
        ) > 1
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE 
        i.value :program :: STRING = 'spl-token'
    AND i.value :programId :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
    AND i.value :parsed :type :: STRING = 'transfer'

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    e.block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
            FROM
                {{ this }}
        )
{% elif is_incremental() %}
AND
    e._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
AND
    e.block_timestamp :: DATE BETWEEN '2022-01-08' -- no ME V2 contract before this date
    AND '2022-02-08'
{% endif %}

),
base AS (
    SELECT
        *
    FROM
        base_tmp
    WHERE
        inner_instruction_type = 'transfer'
        AND COALESCE(
            preceding_inner_instruction_type,
            ''
        ) <> 'create'
),
post_token_balances AS (
    SELECT
        DISTINCT tx_id,
        account,
        mint
    FROM
        {{ ref('silver___post_token_balances2') }}

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
                COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2022-01-08')),'2022-10-05')
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
    block_timestamp :: DATE BETWEEN '2022-01-08' -- no ME V2 contract before this date
    AND '2022-02-08'
{% endif %}
)
SELECT
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.succeeded,
    b.program_id,
    COALESCE(
        b.nft_account_mint,
        p.mint
    ) AS mint,
    b.purchaser,
    ss.seller, 
    SUM(
        b.amount
    ) / pow(
        10,
        9
    ) AS sales_amount,
    b._inserted_timestamp
FROM
    base b
    LEFT OUTER JOIN post_token_balances p
    ON p.tx_id = b.tx_id
    AND p.account = b.nft_account
    LEFT OUTER JOIN sellers ss
    ON ss.tx_id = b.tx_id
GROUP BY
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.succeeded,
    b.program_id,
    COALESCE(
        b.nft_account_mint,
        p.mint
    ),
    b.purchaser,
    ss.seller, 
    b._inserted_timestamp
