{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'delete+insert',
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
            'stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi' -- blazestake stake pool
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
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
stake_events AS (
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
unstake_events AS (
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
)
SELECT
    e.tx_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'delegate' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    e.instruction :accounts [3] :: STRING AS delegator_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp
FROM
    deposit_events e,
    TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :info :lamports IS NOT NULL
UNION
SELECT
    e.tx_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'withdraw' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    e.instruction :accounts [5] :: STRING AS delegator_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp
FROM
    withdraw_events e,
    TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :info :lamports IS NOT NULL
UNION
SELECT
    e.tx_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'stake' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    e.instruction :accounts [1] :: STRING AS delegator_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp
FROM
    stake_events e,
    TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :info :lamports IS NOT NULL
UNION
SELECT
    e.tx_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'unstake' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    e.instruction :accounts [1] :: STRING AS delegator_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp
FROM
    unstake_events e,
    TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :info :lamports IS NOT NULL
