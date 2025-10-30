{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","index"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, program_id, tx_from, tx_to, mint)'),
    full_refresh = false,
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_core']
) }}

{% if execute %}

    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(_inserted_timestamp) AS max_inserted_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_inserted_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

WITH base_transfers_i AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        signers,
        INDEX::string as index,
        event_type,
        program_id,
        instruction,
        inner_instruction,
        succeeded,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
    event_type IN (
        'transfer',
        'transferChecked',
        'transferWithSeed',
        'transferCheckedWithFee'
    )
    AND succeeded
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
AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% else %}
AND
    block_id between 4260184 and 5260184
{% endif %}

    UNION
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        signers,
        CONCAT(
            instruction_index,
            '.',
            inner_index
        ) AS INDEX,
        event_type,
        program_id,
        instruction,
        NULL AS inner_instruction,
        succeeded,
        _inserted_timestamp
    FROM
        {{ ref('silver__events_inner') }}
    WHERE
        event_type IN (
        'transfer',
        'transferChecked',
        'transferWithSeed',
        'transferCheckedWithFee'
        )
    AND succeeded
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
AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
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
        {{ ref('silver___post_token_balances') }}


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
WHERE _inserted_timestamp >= '{{ max_inserted_timestamp }}'
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
        {{ ref('silver___pre_token_balances') }}

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
WHERE _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% else %}
WHERE
    block_id between 4260184 and 5260184
{% endif %}
AND succeeded
),
base_sol_account_keys AS (
    SELECT
        tx_id,
        account_keys
    FROM
        {{ ref('silver__transactions') }}
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
    WHERE _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
    WHERE
        block_id between 4260184 and 5260184
    {% endif %}
AND succeeded
),
spl_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.signers,  
        e.index,
        e.program_id,
        e.succeeded,
        COALESCE(
            p.owner,
            e.instruction :parsed :info :authority :: STRING,
            e.instruction :parsed :info :multisigAuthority :: STRING
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
            p4.decimal,
            9 -- default to solana decimals
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
            p4.mint,
            iff(p5.tx_id IS NOT NULL,'So11111111111111111111111111111111111111112',NULL)
        ) AS mint,
        instruction :parsed :info :source :: STRING as source_token_account,
        instruction :parsed :info :destination :: STRING as dest_token_account,
        e._inserted_timestamp
    FROM
        base_transfers_i e
        LEFT OUTER JOIN base_pre_token_balances p
        ON e.tx_id = p.tx_id
        AND source_token_account = p.account
        LEFT OUTER JOIN base_post_token_balances p2
        ON e.tx_id = p2.tx_id
        AND dest_token_account = p2.account
        LEFT OUTER JOIN base_post_token_balances p3
        ON e.tx_id = p3.tx_id
        AND source_token_account = p3.account
        LEFT OUTER JOIN base_pre_token_balances p4
        ON e.tx_id = p4.tx_id
        AND dest_token_account = p4.account
        LEFT OUTER JOIN base_sol_account_keys p5
        ON e.tx_id = p5.tx_id
        AND (
            silver.udf_get_account_balances_index(dest_token_account, p5.account_keys) IS NOT NULL
            OR silver.udf_get_account_balances_index(source_token_account, p5.account_keys) IS NOT NULL
        )
    WHERE
        (
            e.instruction :parsed :info :authority :: STRING IS NOT NULL
            OR 
            e.instruction :parsed :info :multisigAuthority :: STRING IS NOT NULL
        )
),
sol_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.signers,  
        e.index,
        e.program_id,
        e.succeeded,
        instruction :parsed :info :source :: STRING AS tx_from,
        instruction :parsed :info :destination :: STRING AS tx_to,
        instruction :parsed :info :lamports / pow(
            10,
            9
        ) AS amount,
        CASE
            WHEN e.program_id = '11111111111111111111111111111111' THEN 'So11111111111111111111111111111111111111111'
            ELSE 'So11111111111111111111111111111111111111112'
        END AS mint,
        NULL as source_token_account,
        NULL as dest_token_account,
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
    signers,  
    program_id,
    succeeded,
    INDEX,
    tx_from,
    tx_to,
    amount,
    mint,
    source_token_account,
    dest_token_account,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'index']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    spl_transfers
UNION
SELECT
    block_id,
    block_timestamp,
    tx_id,
    signers,  
    program_id,
    succeeded,
    INDEX,
    tx_from,
    tx_to,
    amount,
    mint,
    source_token_account,
    dest_token_account,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'index']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    sol_transfers
