{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core'],
    full_refresh = false,
    enabled = false,
) }}

WITH base_lido_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'CrX7kMhLC3cSsXJdT7JDgqrRVWGnUpX3gfEfxxU2NVLi'

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
        base_lido_events
    WHERE
        ARRAY_SIZE(
            instruction :accounts
        ) = 8
        AND instruction :accounts [6] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND instruction :accounts [7] = '11111111111111111111111111111111'
),
withdraw_events AS (
    SELECT
        *
    FROM
        base_lido_events
    WHERE
        ARRAY_SIZE(
            instruction :accounts
        ) = 12
        AND instruction :accounts [8] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND instruction :accounts [9] = 'SysvarC1ock11111111111111111111111111111111'
        AND instruction :accounts [10] = '11111111111111111111111111111111'
        AND instruction :accounts [11] = 'Stake11111111111111111111111111111111111111'
)
--,
-- stake_events AS (
--     select *
--     from base_lido_events
--     where ARRAY_SIZE(instruction :accounts) = 13
--     and instruction:accounts[7] = 'SysvarC1ock11111111111111111111111111111111'
--     and instruction:accounts[8] = '11111111111111111111111111111111'
--     and instruction:accounts[9] = 'SysvarRent111111111111111111111111111111111'
--     and instruction:accounts[10] = 'Stake11111111111111111111111111111111111111'
--     and instruction:accounts[11] = 'SysvarStakeHistory1111111111111111111111111'
--     and instruction:accounts[12] = 'StakeConfig11111111111111111111111111111111'
-- ),
-- unstake_events AS (
--     SELECT
--         *
--     FROM
--         base_lido_events
--     WHERE
--         ARRAY_SIZE(
--             instruction :accounts
--         ) = 9
--         AND instruction :accounts [6] = 'SysvarC1ock11111111111111111111111111111111'
--         AND instruction :accounts [7] = '11111111111111111111111111111111'
--         AND instruction :accounts [8] = 'Stake11111111111111111111111111111111111111'
-- )
SELECT
    e.tx_id,
    e.block_id,
    e.block_timestamp,
    e.index,
    e.succeeded,
    'deposit' AS action,
    e.instruction :accounts [0] :: STRING AS stake_pool,
    NULL AS stake_pool_withdraw_authority,
    NULL AS stake_pool_deposit_authority,
    e.instruction :accounts [1] :: STRING AS address,
    e.instruction :accounts [4] :: STRING AS reserve_stake_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp,
    concat_ws('-',tx_id,e.index) as _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['e.tx_id', 'e.index']
    ) }} AS stake_pool_actions_lido_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
    e.instruction :accounts [7] :: STRING AS stake_pool_withdraw_authority,
    NULL AS stake_pool_deposit_authority,
    e.instruction :accounts [1] :: STRING AS address,
    NULL AS reserve_stake_address,
    i.value :parsed :info :lamports AS amount,
    e._inserted_timestamp,
    concat_ws('-',tx_id,e.index) as _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['e.tx_id', 'e.index']
    ) }} AS stake_pool_actions_lido_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id

FROM
    withdraw_events e,
    TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :info :lamports IS NOT NULL
-- UNION
-- SELECT
--     e.tx_id,
--     e.block_timestamp,
--     e.index,
--     e.succeeded,
--     'deposit_stake' AS action,
--     NULL AS delegator_address,
--     i.value :parsed :info :lamports AS amount,
--     e._inserted_timestamp,
--     concat_ws('-',tx_id,e.index) as _unique_key
-- FROM
--     stake_events e,
--     TABLE(FLATTEN(inner_instruction :instructions)) i
-- WHERE
--     i.value :parsed :info :lamports IS NOT NULL
-- UNION
-- SELECT
--     e.tx_id,
--     e.block_timestamp,
--     e.index,
--     e.succeeded,
--     'unstake' AS action,
--     NULL AS delegator_address,
--     i.value :parsed :info :lamports AS amount,
--     e._inserted_timestamp,
--     concat_ws('-',tx_id,e.index) as _unique_key
-- FROM
--     unstake_events e,
--     TABLE(FLATTEN(inner_instruction :instructions)) i
-- WHERE
--     i.value :parsed :info :lamports IS NOT NULL
