{{ config(
    materialized = 'incremental',
    unique_key = "stake_pool_actions_jito_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}

WITH base_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            program_id = 'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy'
            OR ARRAY_CONTAINS(
                'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy' :: variant,
                inner_instruction_program_ids
            )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    -- and block_timestamp :: DATE between '2024-02-01' and '2024-02-10'
    AND block_timestamp :: DATE >= '2022-10-30'
{% endif %}
),
base_stake_pool_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        -1 AS inner_index,
        program_id,
        instruction AS instruction_temp,
        inner_instruction,
        instruction :accounts AS accounts,
        ARRAY_SIZE(accounts) AS num_accounts,
        _inserted_timestamp,
        signers
    FROM
        base_events
    WHERE
        program_id = 'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy'
        AND instruction :accounts [0] :: STRING = 'Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb'
    UNION ALL
    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        e.succeeded,
        e.index,
        i.index AS inner_index,
        i.value :programId :: STRING AS program_id,
        i.value AS instruction_temp,
        NULL AS inner_instruction,
        i.value :accounts AS accounts,
        ARRAY_SIZE(accounts) AS num_accounts,
        e._inserted_timestamp,
        e.signers
    FROM
        base_events e,
        TABLE(FLATTEN(e.inner_instruction :instructions)) i
    WHERE
        i.value :programId :: STRING = 'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy'
        AND i.value :accounts [0] :: STRING = 'Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb'
),
deposit_events AS (
    SELECT
        *
    FROM
        base_stake_pool_events
    WHERE
        num_accounts IN (
            10,
            11
        )
        AND accounts [8] :: STRING = '11111111111111111111111111111111'
        AND accounts [9] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
),
deposit_stake_events AS (
    SELECT
        *
    FROM
        base_stake_pool_events
    WHERE
        num_accounts = 15
        AND accounts [11] :: STRING = 'SysvarC1ock11111111111111111111111111111111'
        AND accounts [12] :: STRING = 'SysvarStakeHistory1111111111111111111111111'
        AND accounts [13] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND accounts [14] :: STRING = 'Stake11111111111111111111111111111111111111'
),
inner_deposit_transfers AS (
    SELECT
        t.block_id,
        t.block_timestamp,
        t.tx_id,
        COALESCE(SPLIT_PART(t.index :: text, '.', 1) :: INT, t.index :: INT) AS INDEX,
        NULLIF(SPLIT_PART(t.index :: text, '.', 2), '') :: INT AS inner_index,
        t.tx_from,
        t.tx_to,
        t.amount
    FROM
        {{ ref('silver__transfers') }} t
        INNER JOIN (
            SELECT
                DISTINCT tx_id,
                block_timestamp :: DATE AS b_date,
                inner_index
            FROM
                deposit_events
            WHERE
                inner_index <> -1
        ) e
        ON e.b_date = t.block_timestamp :: DATE
        AND e.tx_id = t.tx_id
        AND t.program_id = '11111111111111111111111111111111'
        AND mint = 'So11111111111111111111111111111111111111112'

{% if is_incremental() %}
WHERE
    t._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    -- t.block_timestamp :: DATE between '2024-02-01' and '2024-02-10'
    t.block_timestamp :: DATE >= '2022-10-30'
{% endif %}
),
withdraw_events AS (
    SELECT
        *
    FROM
        base_stake_pool_events
    WHERE
        num_accounts IN (
            12,
            13
        )
        AND accounts [8] :: STRING = 'SysvarC1ock11111111111111111111111111111111'
        AND accounts [9] :: STRING = 'SysvarStakeHistory1111111111111111111111111'
        AND accounts [10] :: STRING = 'Stake11111111111111111111111111111111111111'
        AND accounts [11] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
),
-- multiple withdraws in inner_instruction can be exactly the same so get the range to relate to correct Spoo1... event
withdraw_events_inner_program_range AS (
    SELECT
        *,
        CASE
            WHEN inner_index <> -1 THEN inner_index
        END AS start_inner_program,
        CASE
            WHEN inner_index = -1 THEN NULL
            ELSE (
                COALESCE(LEAD(inner_index) over (PARTITION BY tx_id, INDEX
                ORDER BY
                    inner_index) -1, 999999)
            )
        END AS end_inner_program
    FROM
        withdraw_events
),
withdraw_stake_events AS (
    SELECT
        *
    FROM
        base_stake_pool_events
    WHERE
        ARRAY_SIZE(
            accounts
        ) = 13
        AND accounts [10] :: STRING = 'SysvarC1ock11111111111111111111111111111111'
        AND accounts [11] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND accounts [12] :: STRING = 'Stake11111111111111111111111111111111111111'
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
                deposit_stake_events
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
    -- t.block_timestamp :: DATE between '2024-02-01' and '2024-02-10'
    t.block_timestamp :: DATE >= '2022-10-30'
{% endif %}
),
merge_events AS (
    SELECT
        b.tx_id,
        b.index,
        i.index AS inner_index,
        i.value :parsed :info :destination :: STRING AS merge_destination,
        i.value :parsed :info :stakeAuthority :: STRING AS temp_stake_authority
    FROM
        base_events b,
        TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        i.value :parsed :type = 'merge'
        AND i.value :programId = 'Stake11111111111111111111111111111111111111'
        AND b.tx_id IN (
            SELECT
                tx_id
            FROM
                deposit_stake_events
        )
),
deposit_stake_merge AS (
    SELECT
        e.tx_id,
        e.block_id,
        e.block_timestamp,
        e.index,
        e.inner_index,
        e.succeeded,
        e.accounts [0] :: STRING AS stake_pool,
        e.accounts [3] :: STRING AS stake_pool_withdraw_authority,
        e.accounts [2] :: STRING AS stake_pool_deposit_authority,
        b.signers [0] :: STRING AS address,
        e.accounts [6] :: STRING AS reserve_stake_address,
        i.merge_destination,
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
        LEFT OUTER JOIN merge_events i
        ON e.tx_id = i.tx_id
        AND e.index = i.index
    WHERE
        amount IS NOT NULL
        AND i.temp_stake_authority = stake_pool_withdraw_authority
        AND i.merge_destination = e.accounts [5] :: STRING
),
pre_final AS (
    SELECT
        e.tx_id,
        e.block_id,
        e.block_timestamp,
        e.index,
        e.inner_index,
        e.succeeded,
        'deposit' AS action,
        e.accounts [0] :: STRING AS stake_pool,
        e.accounts [1] :: STRING AS stake_pool_withdraw_authority,
        NULL AS stake_pool_deposit_authority,
        e.signers [0] :: STRING AS address,
        e.accounts [2] :: STRING AS reserve_stake_address,
        (t.amount * pow(10, 9)) :: INT AS amount,
        e._inserted_timestamp
    FROM
        deposit_events e
        LEFT JOIN inner_deposit_transfers t
        ON e.tx_id = t.tx_id
        AND e.accounts [3] :: STRING = t.tx_from
        and e.index = t.index
        and e.inner_index = (t.inner_index - 1)
    where
        e.inner_index <> -1
    UNION
    SELECT
        e.tx_id,
        e.block_id,
        e.block_timestamp,
        e.index,
        e.inner_index,
        e.succeeded,
        'deposit' AS action,
        e.accounts [0] :: STRING AS stake_pool,
        e.accounts [1] :: STRING AS stake_pool_withdraw_authority,
        NULL AS stake_pool_deposit_authority,
        e.signers [0] :: STRING AS address,
        -- use signers instead of instruction account because of "passthrough" wallets
        e.accounts [2] :: STRING AS reserve_stake_address,
        i.value :parsed :info :lamports AS amount,
        e._inserted_timestamp
    FROM
        deposit_events e
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        i.value :parsed :info :lamports IS NOT NULL
    UNION
    SELECT
        e.tx_id,
        e.block_id,
        e.block_timestamp,
        e.index,
        e.inner_index,
        e.succeeded,
        'withdraw' AS action,
        e.accounts [0] :: STRING AS stake_pool,
        e.accounts [1] :: STRING AS stake_pool_withdraw_authority,
        NULL AS stake_pool_deposit_authority,
        e.accounts [5] :: STRING AS address,
        e.accounts [4] :: STRING AS reserve_stake_address,
        i.value :parsed :info :lamports AS amount,
        e._inserted_timestamp 
    FROM
        withdraw_events_inner_program_range e
        LEFT OUTER JOIN base_events b
        ON e.tx_id = b.tx_id
        AND e.index = b.index
        LEFT OUTER JOIN TABLE(FLATTEN(b.inner_instruction :instructions)) i
    WHERE
        i.value :parsed :type = 'withdraw'
        AND i.value :parsed :info :withdrawAuthority = stake_pool_withdraw_authority
        AND i.value :parsed :info :lamports IS NOT NULL
        AND (
            e.start_inner_program IS NULL
            OR i.index BETWEEN e.start_inner_program
            AND e.end_inner_program
        )
    UNION
    SELECT
        e.tx_id,
        e.block_id,
        e.block_timestamp,
        e.index,
        e.inner_index,
        e.succeeded,
        'deposit_stake' AS action,
        e.stake_pool,
        e.stake_pool_withdraw_authority,
        e.stake_pool_deposit_authority,
        e.address,
        e.reserve_stake_address,
        e.amount :: NUMBER AS amount,
        e._inserted_timestamp
    FROM
        deposit_stake_merge e
    UNION
    SELECT
        e.tx_id,
        e.block_id,
        e.block_timestamp,
        e.index,
        e.inner_index,
        e.succeeded,
        'withdraw_stake' AS action,
        e.accounts [0] :: STRING AS stake_pool,
        e.accounts [2] :: STRING AS stake_pool_withdraw_authority,
        NULL AS stake_pool_deposit_authority,
        e.accounts [5] :: STRING AS address,
        NULL AS reserve_stake_address,
        i.value :parsed :info :lamports AS amount,
        e._inserted_timestamp
    FROM
        withdraw_stake_events e
        LEFT OUTER JOIN base_events b
        ON e.tx_id = b.tx_id
        AND e.index = b.index
        LEFT OUTER JOIN TABLE(FLATTEN(b.inner_instruction :instructions)) i
    WHERE
        i.value :parsed :info :lamports IS NOT NULL
        AND i.value :parsed :type :: STRING = 'split'
        AND i.value :parsed :info :newSplitAccount = e.accounts [4]
)
SELECT
    tx_id,
    block_id,
    block_timestamp,
    INDEX,
    inner_index,
    succeeded,
    action,
    stake_pool,
    stake_pool_withdraw_authority,
    stake_pool_deposit_authority,
    address,
    reserve_stake_address,
    amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'index', 'inner_index']
    ) }} AS stake_pool_actions_jito_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
