{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    full_refresh = false
) }}

WITH base_e AS (

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
        {{ ref('silver__events') }}
    WHERE
        program_id = 'Stake11111111111111111111111111111111111111'

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+1,151738154)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+4000000,151738154)
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
    block_id between 105368 and 1000000
{% endif %}
    UNION
    SELECT
        i.block_id,
        i.block_timestamp,
        i.tx_id,
        CONCAT(
            i.mapped_instruction_index,
            '.',
            ii.index
        ) AS INDEX,
        ii.value :parsed :type :: STRING AS event_type,
        ii.value :programId :: STRING AS program_id,
        ii.value AS instruction,
        NULL AS inner_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver___inner_instructions') }}
        i,
        TABLE(FLATTEN(i.value :instructions)) ii
    WHERE
        ii.value :programId :: STRING = 'Stake11111111111111111111111111111111111111'

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    i.block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+1,151738154)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+4000000,151738154)
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
    block_id between 105368 and 1000000
{% endif %}
),
base_t AS (
    SELECT
        block_id,
        tx_id,
        succeeded,
        signers,
        pre_balances,
        post_balances,
        pre_token_balances,
        post_token_balances,
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
            LEAST(COALESCE(MAX(block_id), 105368)+1,151738154)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+4000000,151738154)
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
    block_id between 105368 and 1000000
{% endif %}
)
SELECT
    e.block_id,
    e.block_timestamp,
    e.tx_id,
    t.succeeded,
    e.index,
    e.event_type,
    e.program_id,
    t.signers,
    t.account_keys,
    e.instruction,
    e.inner_instruction,
    t.pre_balances,
    t.post_balances,
    t.pre_token_balances,
    t.post_token_balances,
    e._inserted_timestamp
FROM
    base_e e
    LEFT OUTER JOIN base_t t
    ON t.block_id = e.block_id
    AND t.tx_id = e.tx_id
