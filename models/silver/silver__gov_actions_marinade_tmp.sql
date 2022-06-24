{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH token_balances AS (

    SELECT
        tx_id,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }}
    WHERE
        mint = 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND _inserted_timestamp :: DATE >= '2022-04-01' -- no marinade gov before this date
{% endif %}
UNION
SELECT
    tx_id,
    account,
    mint,
    DECIMAL
FROM
    {{ ref('silver___pre_token_balances') }}
WHERE
    mint = 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND _inserted_timestamp :: DATE >= '2022-04-01' -- no marinade gov before this date
{% endif %}
),
marinade_lock_txs AS (
    SELECT
        DISTINCT e.tx_id,
        e.succeeded
    FROM
        {{ ref('silver__events') }}
        e
        LEFT OUTER JOIN TABLE(
            FLATTEN(
                input => inner_instruction :instructions,
                outer => TRUE
            )
        ) ii
    WHERE
        program_id = 'tovt1VkTE2T4caWoeFP6a2xSFoew5mNpd7FWidyyMuk'

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND e._inserted_timestamp :: DATE >= '2022-04-01'
{% endif %}
EXCEPT
SELECT
    DISTINCT e.tx_id,
    e.succeeded
FROM
    {{ ref('silver__events') }}
    e
    LEFT OUTER JOIN TABLE(
        FLATTEN(
            input => inner_instruction :instructions,
            outer => TRUE
        )
    ) ii
WHERE
    (
        program_id = 'Govz1VyoyLD5BL6CSCxUJLVLsQHRwjfFj1prNsdNg5Jw' -- ignore votes
        OR COALESCE(
            ii.value :programId :: STRING,
            ''
        ) = 'Govz1VyoyLD5BL6CSCxUJLVLsQHRwjfFj1prNsdNg5Jw'
    )

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND e._inserted_timestamp :: DATE >= '2022-04-01'
{% endif %}
),
b AS (
    SELECT
        t.tx_id,
        l.index,
        l.value :: STRING AS log_message,
        CASE
            WHEN l.value :: STRING LIKE '%invoke%' THEN 1
            WHEN l.value :: STRING LIKE '%success' THEN -1
            ELSE 0
        END AS cnt,
        SUM(cnt) over (
            PARTITION BY t.tx_id
            ORDER BY
                l.index rows BETWEEN unbounded preceding
                AND CURRENT ROW
        ) AS event_cumsum
    FROM
        {{ ref('silver__transactions') }}
        t
        INNER JOIN marinade_lock_txs d
        ON t.tx_id = d.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l

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
    t._inserted_timestamp :: DATE >= '2022-04-01'
{% endif %}
),
C AS (
    SELECT
        b.*,
        LAG(
            event_cumsum,
            1
        ) over (
            PARTITION BY tx_id
            ORDER BY
                INDEX
        ) AS prev_event_cumsum
    FROM
        b
),
tx_logs AS (
    SELECT
        C.tx_id,
        C.index AS log_index,
        C.log_message,
        CASE
            WHEN C.log_message = 'Program log: Instruction: CreateSimpleNftEscrow' THEN 'MINT LOCK'
            WHEN C.log_message = 'Program log: Instruction: UpdateLockAmount' THEN 'UPDATE LOCK'
            WHEN C.log_message = 'Program log: Instruction: StartUnlocking' THEN 'START UNLOCK'
            WHEN C.log_message = 'Program log: Instruction: CancelUnlocking' THEN 'CANCEL UNLOCK'
            WHEN C.log_message = 'Program log: Instruction: ExitEscrow' THEN 'EXIT'
            ELSE NULL
        END AS action,
        conditional_true_event(
            prev_event_cumsum = 0
        ) over (
            PARTITION BY tx_id
            ORDER BY
                INDEX
        ) AS event_index
    FROM
        C
),
actions_tmp AS (
    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        m.succeeded,
        e.index,
        LAST_VALUE(
            action ignore nulls
        ) over (
            PARTITION BY e.tx_id
            ORDER BY
                e.index
        ) AS main_action,
        CASE
            WHEN main_action LIKE '% LOCK' THEN e.instruction :parsed :info :amount * pow(
                10,
                -9
            ) :: FLOAT
            WHEN main_action = 'EXIT' THEN e.inner_instruction :instructions [0] :parsed :info :amount * pow(
                10,
                -9
            ) :: FLOAT
        END AS lock_amount,
        CASE
            WHEN main_action LIKE '% LOCK'
            OR main_action = 'EXIT' THEN COALESCE(
                e.instruction :accounts [5] :: STRING,
                e.instruction :parsed :info :authority :: STRING
            )
            WHEN main_action IN (
                'START UNLOCK',
                'CANCEL UNLOCK'
            ) THEN e.instruction :accounts [4] :: STRING
        END AS locker,
        CASE
            WHEN main_action LIKE '% LOCK'
            OR main_action = 'EXIT' THEN COALESCE(
                e.instruction :accounts [6] :: STRING,
                e.instruction :parsed :info :destination :: STRING
            )
            ELSE NULL
        END AS locker_account_tmp,
        MIN(
            e.index
        ) over (
            PARTITION BY e.tx_id
            ORDER BY
                e.index
        ) AS min_index,
        CASE
            WHEN main_action IN (
                'MINT LOCK',
                'EXIT'
            ) THEN e.instruction :accounts [2] :: STRING
            WHEN main_action IN (
                'START UNLOCK',
                'CANCEL UNLOCK'
            ) THEN e.instruction :accounts [1] :: STRING
            ELSE NULL
        END AS locker_nft,
        e.ingested_at,
        e._inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN marinade_lock_txs m
        ON m.tx_id = e.tx_id
        LEFT OUTER JOIN tx_logs l
        ON e.tx_id = l.tx_id
        AND e.index = l.event_index
        AND l.action IS NOT NULL
    WHERE
        (
            e.event_type = 'transfer'
            OR l.action IS NOT NULL
        )

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND e._inserted_timestamp :: DATE >= '2022-04-01'
{% endif %}
)
SELECT
    a1.block_timestamp,
    a1.block_id,
    a1.tx_id,
    a1.succeeded,
    COALESCE(
        a1.locker,
        a2.locker
    ) AS signer,
    COALESCE(
        a1.locker_account_tmp,
        a2.locker_account_tmp
    ) AS locker_account,
    COALESCE(
        a1.locker_nft,
        a2.locker_nft
    ) AS locker_nft,
    b.mint,
    a1.main_action AS action,
    COALESCE(
        a1.lock_amount,
        a2.lock_amount
    ) AS amount,
    a1.ingested_at,
    a1._inserted_timestamp
FROM
    actions_tmp a1
    LEFT OUTER JOIN actions_tmp a2
    ON a1.tx_id = a2.tx_id
    AND a1.index <> a2.index
    LEFT OUTER JOIN token_balances b
    ON a1.tx_id = b.tx_id
    AND COALESCE(
        a1.locker_account_tmp,
        a2.locker_account_tmp
    ) = b.account
WHERE
    a1.index = a1.min_index
