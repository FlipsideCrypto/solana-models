{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

With base_transfers_i AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        INDEX,
        event_type,
        program_id,
        instruction,
        inner_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events2') }}
    WHERE
    event_type IN (
        'transfer',
        'transferChecked'
    )
    AND succeeded = TRUE

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 4260184)+1,151386092)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 4260184)+4000000,151386092)
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
    block_id between 4260184 and 5260184
{% endif %}

    UNION
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        CONCAT(
            e.inner_instruction :index :: NUMBER,
            '.',
            ii.index
        ) AS INDEX,
        ii.value :parsed :type :: STRING AS event_type,
        ii.value :programId :: STRING AS program_id,
        ii.value as instruction,
        NULL AS inner_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events2') }}
        e,
        TABLE(FLATTEN(e.inner_instruction :instructions)) ii
    WHERE
        e.succeeded = TRUE
        AND
        ii.value :parsed :type :: STRING IN (
        'transfer',
        'transferChecked'
        )

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 4260184)+1,151386092)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 4260184)+4000000,151386092)
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
    block_id between 4260184 and 5260184
{% endif %}
),
base_post_token_balances AS (
    SELECT
        tx_id,
        owner,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances2') }}


{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 4260184)+1,151386092)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 4260184)+4000000,151386092)
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
    block_id between 4260184 and 5260184
{% endif %}
),
base_pre_token_balances AS (
    SELECT
        tx_id,
        owner,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___pre_token_balances2') }}

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 4260184)+1,151386092)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 4260184)+4000000,151386092)
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
    block_id between 4260184 and 5260184
{% endif %}
),
spl_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        COALESCE(
            p.owner,
            e.instruction :parsed :info :authority :: STRING
        ) AS tx_from,
        COALESCE(
            p2.owner,
            instruction :parsed :info :destination :: STRING
        ) AS tx_to,
        COALESCE(
            e.instruction :parsed :info :tokenAmount: decimals,
            p.decimal,
            p2.decimal,
            p3.decimal,
            p4.decimal
        ) AS decimal_adj,
        COALESCE (
            e.instruction :parsed :info :amount :: INTEGER,
            e.instruction :parsed :info :tokenAmount :amount :: INTEGER
        ) / pow(
            10,
            decimal_adj
        ) AS amount,
        COALESCE(
            p.mint,
            p2.mint,
            p3.mint,
            p4.mint
        ) AS mint,
        e._inserted_timestamp
    FROM
        base_transfers_i e
        LEFT OUTER JOIN base_pre_token_balances p
        ON e.tx_id = p.tx_id
        AND e.instruction :parsed :info :source :: STRING = p.account
        LEFT OUTER JOIN base_post_token_balances p2
        ON e.tx_id = p2.tx_id
        AND e.instruction :parsed :info :destination :: STRING = p2.account
        LEFT OUTER JOIN base_post_token_balances p3
        ON e.tx_id = p3.tx_id
        AND e.instruction :parsed :info :source :: STRING = p3.account
        LEFT OUTER JOIN base_pre_token_balances p4
        ON e.tx_id = p4.tx_id
        AND e.instruction :parsed :info :destination :: STRING = p4.account
    WHERE
        e.instruction :parsed :info :authority :: STRING IS NOT NULL
),
sol_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        instruction :parsed :info :source :: STRING AS tx_from,
        instruction :parsed :info :destination :: STRING AS tx_to,
        instruction :parsed :info :lamports / pow(
            10,
            9
        ) AS amount,
        'So11111111111111111111111111111111111111112' AS mint,
        e._inserted_timestamp
    FROM
        base_transfers_i e
    WHERE
        instruction :parsed :info :lamports :: STRING IS NOT NULL
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    INDEX,
    tx_from,
    tx_to,
    amount,
    mint,
    _inserted_timestamp
FROM
    spl_transfers
UNION
SELECT
    block_id,
    block_timestamp,
    tx_id,
    INDEX,
    tx_from,
    tx_to,
    amount,
    mint,
    _inserted_timestamp
FROM
    sol_transfers
