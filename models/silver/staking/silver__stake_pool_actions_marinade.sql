{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}

WITH base_marinade_stake_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-08-01'
{% endif %}
),
base_balances AS (
    SELECT
        t.tx_id,
        signers,
        pre_balances,
        post_balances,
        account_keys
    FROM
        {{ ref('silver__transactions') }}
        t
        INNER JOIN (
            SELECT
                DISTINCT tx_id,
                block_timestamp :: DATE AS b_date
            FROM
                base_marinade_stake_events
        ) e
        ON e.b_date = t.block_timestamp :: DATE
        AND e.tx_id = t.tx_id

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    t.block_timestamp :: DATE >= '2021-08-01'
{% endif %}
),
deposit_events AS (
    SELECT
        *
    FROM
        base_marinade_stake_events
    WHERE
        ARRAY_SIZE(
            instruction :accounts
        ) = 11
        AND instruction :accounts [9] = '11111111111111111111111111111111'
        AND instruction :accounts [10] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
),
deposit_stake_events AS (
    SELECT
        *
    FROM
        base_marinade_stake_events
    WHERE
        ARRAY_SIZE(
            instruction :accounts
        ) = 15
        AND instruction :accounts [10] :: STRING = 'SysvarC1ock11111111111111111111111111111111'
        AND instruction :accounts [11] :: STRING = 'SysvarRent111111111111111111111111111111111'
        AND instruction :accounts [12] :: STRING = '11111111111111111111111111111111'
        AND instruction :accounts [13] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND instruction :accounts [14] :: STRING = 'Stake11111111111111111111111111111111111111'
),
order_unstake_events AS (
    SELECT
        *
    FROM
        base_marinade_stake_events
    WHERE
        ARRAY_SIZE(
            instruction :accounts
        ) = 8
        AND instruction :accounts [5] = 'SysvarC1ock11111111111111111111111111111111'
        AND instruction :accounts [6] = 'SysvarRent111111111111111111111111111111111'
        AND instruction :accounts [7] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
),
claim_events AS (
    SELECT
        *
    FROM
        base_marinade_stake_events
    WHERE
        ARRAY_SIZE(
            instruction :accounts
        ) = 6
        AND instruction :accounts [4] = 'SysvarC1ock11111111111111111111111111111111'
        AND instruction :accounts [5] = '11111111111111111111111111111111'
),
deposit_stake_authorize AS (
    SELECT
        e.tx_id,
        e.block_id,
        e.block_timestamp,
        e.index,
        e.succeeded,
        e.instruction :accounts [0] :: STRING AS stake_pool,
        NULL AS stake_pool_withdraw_authority,
        i.value :parsed :info :newAuthority :: STRING AS stake_pool_deposit_authority,
        e.instruction :accounts [4] :: STRING AS address,
        NULL AS reserve_stake_address,
        e.instruction :accounts [3] :: STRING AS stake_account,
        silver.udf_get_account_balances_index(
            stake_account,
            b.account_keys
        ) AS stake_account_balances_index,
        b.post_balances [stake_account_balances_index] AS amount,
        e._inserted_timestamp
    FROM
        deposit_stake_events e
        LEFT OUTER JOIN base_balances b
        ON b.tx_id = e.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        i.value :parsed :type :: STRING = 'authorize'
        AND i.value :parsed :info :authorityType :: STRING = 'Staker'
        AND i.value :programId = 'Stake11111111111111111111111111111111111111'
),
pre_final as (
SELECT
    e.tx_id,
    e.block_id,
    e.block_timestamp,
    CONCAT(
        e.index,
        '.',
        i.index
    ) AS INDEX,
    e.succeeded,
    'deposit' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    NULL AS stake_pool_withdraw_authority,
    NULL AS stake_pool_deposit_authority,
    e.instruction :accounts [6] :: STRING AS address,
    e.instruction :accounts [5] :: STRING AS reserve_stake_address,
    NULL AS claim_ticket_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp,
    'SOL' as token,
    concat_ws(
        '-',
        tx_id,
        CONCAT(
            e.index,
            '.',
            i.index
        )
    ) AS _unique_key
FROM
    deposit_events e,
    TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :info :lamports IS NOT NULL
UNION
SELECT
    e.tx_id,
    e.block_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'deposit_stake' AS action,
    e.stake_pool,
    e.stake_pool_withdraw_authority,
    e.stake_pool_deposit_authority,
    e.address,
    e.reserve_stake_address,
    NULL AS claim_ticket_address,
    e.amount,
    e._inserted_timestamp,
    'SOL' as token,
    concat_ws(
        '-',
        tx_id,
        e.index
    ) AS _unique_key
FROM
    deposit_stake_authorize e
UNION
SELECT
    e.tx_id,
    e.block_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'order_unstake' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    NULL AS stake_pool_withdraw_authority,
    NULL AS stake_pool_deposit_authority,
    e.instruction :accounts [3] :: STRING AS address,
    NULL AS reserve_stake_address,
    e.instruction :accounts [4] :: STRING AS claim_ticket_address,
    e.inner_instruction:instructions[0]:parsed:info:amount::int as amount,
    e._inserted_timestamp,
    'mSOL' as token,
    concat_ws(
        '-',
        tx_id,
        e.index
    ) AS _unique_key
FROM
    order_unstake_events e
UNION
SELECT
    e.tx_id,
    e.block_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'claim' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    NULL AS stake_pool_withdraw_authority,
    NULL AS stake_pool_deposit_authority,
    e.instruction :accounts [3] :: STRING AS address,
    e.instruction :accounts [1] :: STRING AS reserve_stake_address,
    e.instruction :accounts [2] :: STRING AS claim_ticket_address,
    (
        i.value :parsed :info :lamports + b.pre_balances [silver.udf_get_account_balances_index( claim_ticket_address, b.account_keys)]
    ) :: NUMBER AS amount,
    e._inserted_timestamp,
    'SOL' as token,
    concat_ws(
        '-',
        e.tx_id,
        e.index
    ) AS _unique_key
FROM
    claim_events e
    LEFT OUTER JOIN base_balances b
    ON b.tx_id = e.tx_id
    LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :info :lamports IS NOT NULL
)
SELECT
    tx_id,
    block_id,
    block_timestamp,
    index,
    succeeded,
    action,
    stake_pool,
    stake_pool_withdraw_authority,
    stake_pool_deposit_authority,
    address,
    reserve_stake_address,
    claim_ticket_address,
    amount,
    _inserted_timestamp,
    token,
    _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'index']
    ) }} AS stake_pool_actions_marinade_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
