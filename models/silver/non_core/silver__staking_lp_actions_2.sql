{{
    config(
        materialized = 'incremental',
        unique_key = ['block_timestamp::DATE','staking_lp_actions_2_id'],
        cluster_by = ['block_timestamp::DATE','event_type'],
        tags = ['scheduled_non_core'],
    )
}}

WITH base_e AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        index,
        NULL AS inner_index,
        event_type,
        program_id,
        instruction,
        inner_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'Stake11111111111111111111111111111111111111'
        {% if is_incremental() %}
        AND _inserted_timestamp >= (
            SELECT
                max(_inserted_timestamp)
            FROM
                {{ this }}
        )
        {% else %}
        AND _inserted_timestamp BETWEEN '2022-08-12' AND '2023-01-01'
        {% endif %}
    
    UNION ALL
    
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        instruction_index AS index,
        inner_index,
        instruction:parsed:type::STRING AS event_type,
        instruction:programId::STRING AS program_id,
        instruction,
        NULL AS inner_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events_inner') }}
    WHERE
        program_id = 'Stake11111111111111111111111111111111111111'
        AND instruction:parsed IS NOT NULL
        {% if is_incremental() %}
        AND _inserted_timestamp >= (
            SELECT
                max(_inserted_timestamp)
            FROM
                {{ this }}
        )
        {% else %}
        AND _inserted_timestamp BETWEEN '2022-08-12' AND '2023-01-01'
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
    WHERE
        {% if is_incremental() %}
        _inserted_timestamp >= (
            SELECT
                max(_inserted_timestamp)
            FROM
                {{ this }}
        )
        {% else %}
        _inserted_timestamp BETWEEN '2022-08-12' AND '2023-01-01'
        {% endif %}
        AND succeeded
)

SELECT
    e.block_id,
    e.block_timestamp,
    e.tx_id,
    t.succeeded,
    e.index,
    e.inner_index,
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
    e._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['e.block_id', 'e.tx_id', 'e.index', 'e.inner_index']
    ) }} AS staking_lp_actions_2_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_e e
LEFT OUTER JOIN 
    base_t AS t
    ON t.block_id = e.block_id
    AND t.tx_id = e.tx_id
