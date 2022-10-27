{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::date']
) }}

WITH base_stake_pool_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy'
        AND instruction :accounts [0] :: STRING IN (
            -- daopool stake pool
            '7ge2xKsZXmqPxa3YmXxXmzCp9Hc2ezrTxh6PECaxCwrL',
            -- blazestake stake pool
            'stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi',
            -- jpool stake pool
            'CtMyWsrUtAwXWiGr9WjHT5fC3p3fgV8cyGpLTo2LJzG1'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
AND
    block_timestamp :: DATE >= '2021-10-11'
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
                base_stake_pool_events
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
    t.block_timestamp :: DATE >= '2021-10-11'
{% endif %}
),
deposit_events AS (
    SELECT
        *
    FROM
        base_stake_pool_events
    WHERE
            ARRAY_SIZE(
            instruction :accounts
            ) IN (
                10,
                11
            )
            AND instruction :accounts [8] :: STRING = '11111111111111111111111111111111'
            AND instruction :accounts [9] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
),
deposit_stake_events AS (
    SELECT
        *
    FROM
        base_stake_pool_events
    WHERE
            ARRAY_SIZE(
            instruction :accounts
            ) = 15
            AND instruction :accounts [11] :: STRING = 'SysvarC1ock11111111111111111111111111111111'
            AND instruction :accounts [12] :: STRING = 'SysvarStakeHistory1111111111111111111111111'
            AND instruction :accounts [13] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND instruction :accounts [14] :: STRING = 'Stake11111111111111111111111111111111111111'
),
withdraw_events AS (
    SELECT
        *
    FROM
        base_stake_pool_events
    WHERE
        ARRAY_SIZE(
            instruction :accounts
        ) IN (
            12,
            13
        )
        AND instruction :accounts [8] :: STRING = 'SysvarC1ock11111111111111111111111111111111'
        AND instruction :accounts [9] :: STRING = 'SysvarStakeHistory1111111111111111111111111'
        AND instruction :accounts [10] :: STRING = 'Stake11111111111111111111111111111111111111'
        AND instruction :accounts [11] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
),
withdraw_stake_events AS (
    SELECT
        *
    FROM
        base_stake_pool_events
    WHERE
        ARRAY_SIZE(
        instruction :accounts
        ) = 13
        AND instruction :accounts [10] :: STRING = 'SysvarC1ock11111111111111111111111111111111'
        AND instruction :accounts [11] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND instruction :accounts [12] :: STRING = 'Stake11111111111111111111111111111111111111'
),
increase_validator_stake_events AS (
    select *
    from base_stake_pool_events
    where ARRAY_SIZE(instruction :accounts) = 13
    and instruction:accounts[7] = 'SysvarC1ock11111111111111111111111111111111'
    and instruction:accounts[8] = 'SysvarRent111111111111111111111111111111111'
    and instruction:accounts[9] = 'SysvarStakeHistory1111111111111111111111111'
    and instruction:accounts[10] = 'StakeConfig11111111111111111111111111111111'
    and instruction:accounts[11] = '11111111111111111111111111111111'
    and instruction:accounts[12] = 'Stake11111111111111111111111111111111111111'
),
decrease_validator_stake_events AS (
    SELECT
        *
    FROM
        base_stake_pool_events
    WHERE
        ARRAY_SIZE(instruction :accounts) = 10
    and instruction:accounts[6] = 'SysvarC1ock11111111111111111111111111111111'
    and instruction:accounts[7] = 'SysvarRent111111111111111111111111111111111'
    and instruction:accounts[8] = '11111111111111111111111111111111'
    and instruction:accounts[9] = 'Stake11111111111111111111111111111111111111'
),
deposit_stake_merge AS (
    SELECT
        e.tx_id,
        e.block_id,
        e.block_timestamp,
        e.index,
        e.succeeded,
        e.instruction :accounts [0] :: STRING AS stake_pool,
        e.instruction :accounts [3] :: STRING AS stake_pool_withdraw_authority,
        e.instruction :accounts [2] :: STRING AS stake_pool_deposit_authority,
        b.signers [0] :: STRING AS address,
        e.instruction :accounts [6] :: STRING AS reserve_stake_address,
        i.value :parsed :info :destination :: STRING AS merge_destination,
        silver.udf_get_account_balances_index(
            merge_destination,
            b.account_keys
        ) AS merge_destination_balances_index,
        silver.udf_get_account_balances_index(
            reserve_stake_address,
            b.account_keys
        ) AS reserve_stake_balances_index,
        b.post_balances [merge_destination_balances_index] - b.pre_balances [merge_destination_balances_index] + b.post_balances [reserve_stake_balances_index] - b.pre_balances [reserve_stake_balances_index] AS amount,
        e._inserted_timestamp
    FROM
        deposit_stake_events e
        LEFT OUTER JOIN base_balances b
        ON b.tx_id = e.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        i.value :parsed :type = 'merge'
        AND i.value :programId = 'Stake11111111111111111111111111111111111111'
)
SELECT
    e.tx_id,
    e.block_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'deposit' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    e.instruction :accounts [1] :: STRING AS stake_pool_withdraw_authority,
    NULL as stake_pool_deposit_authority,
    e.instruction :accounts [3] :: STRING AS address,
    e.instruction :accounts [2] :: STRING AS reserve_stake_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp,
    concat_ws('-',tx_id,e.index) as _unique_key
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
    'withdraw' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    e.instruction :accounts [1] :: STRING AS stake_pool_withdraw_authority,
    NULL as stake_pool_deposit_authority,
    e.instruction :accounts [5] :: STRING AS address,
    e.instruction :accounts [4] :: STRING AS reserve_stake_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp,
    concat_ws('-',tx_id,e.index) as _unique_key
FROM
    withdraw_events e,
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
    e.amount :: NUMBER AS amount,
    e._inserted_timestamp,
    concat_ws(
        '-',
        e.tx_id,
        e.index
    ) AS _unique_key
FROM
    deposit_stake_merge e
UNION
SELECT
    e.tx_id,
    e.block_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'withdraw_stake' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    e.instruction :accounts [2] :: STRING AS stake_pool_withdraw_authority,
    NULL as stake_pool_deposit_authority,
    e.instruction :accounts [5] :: STRING AS address,
    NULL AS reserve_stake_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp,
    concat_ws('-',tx_id,e.index) as _unique_key
FROM
    withdraw_stake_events e,
    TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :info :lamports IS NOT NULL
-- UNION
-- SELECT
--     e.tx_id,
--     e.block_id,
--     e.block_timestamp,
--     e.index,
--     e.succeeded,
--     'increase_validator_stake' AS action,
--     e.instruction :accounts [0] :: STRING AS stake_pool,
--     e.instruction :accounts [2] :: STRING AS stake_pool_withdraw_authority,
--     NULL as stake_pool_deposit_authority,
--     NULL AS address,
--     e.instruction :accounts [4] :: STRING AS reserve_stake_address,
--     i.value :parsed :info :lamports AS amount,
--     e._inserted_timestamp,
--     concat_ws('-',tx_id,e.index) as _unique_key
-- FROM
--     increase_validator_stake_events e,
--     TABLE(FLATTEN(inner_instruction :instructions)) i
-- WHERE
--     i.value :parsed :info :lamports IS NOT NULL
-- UNION
-- SELECT
--     e.tx_id,
--     e.block_id,
--     e.block_timestamp,
--     e.index,
--     e.succeeded,
--     'decrease_validator_stake' AS action,
--     e.instruction :accounts [0] :: STRING AS stake_pool,
--     e.instruction :accounts [2] :: STRING AS stake_pool_withdraw_authority,
--     NULL as stake_pool_deposit_authority,
--     NULL AS address,
--     NULL AS reserve_stake_address,
--     i.value :parsed :info :lamports AS amount,
--     e._inserted_timestamp,
--     concat_ws('-',tx_id,e.index) as _unique_key
-- FROM
--     decrease_validator_stake_events e,
--     TABLE(FLATTEN(inner_instruction :instructions)) i
-- WHERE
--     i.value :parsed :info :lamports IS NOT NULL
