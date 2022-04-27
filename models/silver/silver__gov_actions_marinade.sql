{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
) }}

WITH post_token_balances AS (

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
AND ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
marinade_lock_txs AS (
    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__events') }}
        e,
        TABLE(
            FLATTEN(
                input => inner_instruction :instructions,
                outer => TRUE
            )
        ) ii
    WHERE
        program_id = 'tovt1VkTE2T4caWoeFP6a2xSFoew5mNpd7FWidyyMuk'

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
EXCEPT
SELECT
    DISTINCT tx_id
FROM
    {{ ref('silver__events') }}
    e,
    TABLE(
        FLATTEN(
            input => inner_instruction :instructions,
            outer => TRUE
        )
    ) ii
WHERE
    program_id = 'Govz1VyoyLD5BL6CSCxUJLVLsQHRwjfFj1prNsdNg5Jw' -- ignore votes

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
b AS (
    SELECT
        t.tx_id,
        t.succeeded,
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
    t.ingested_at :: DATE >= CURRENT_DATE - 2
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
        C.succeeded,
        C.index AS log_index,
        C.log_message,
        CASE
            WHEN C.log_message = 'Program log: Instruction: CreateSimpleNftEscrow' THEN 'MINT LOCK'
            WHEN C.log_message = 'Program log: Instruction: UpdateLockAmount' THEN 'UPDATE LOCK'
            WHEN C.log_message = 'Program log: Instruction: StartUnlocking' THEN 'START UNLOCK'
            WHEN C.log_message = 'Program log: Instruction: CancelUnlocking' THEN 'CANCEL UNLOCK'
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
        l.succeeded,
        e.index,
        e.instruction :parsed :info :amount * pow(
            10,
            -9
        ) :: FLOAT AS lock_amount,
        LAST_VALUE(
            action ignore nulls
        ) over (
            PARTITION BY e.tx_id
            ORDER BY
                e.index
        ) AS main_action,
        CASE
            WHEN main_action LIKE '% LOCK' THEN COALESCE(
                e.instruction :accounts [5] :: STRING,
                e.instruction :parsed :info :authority :: STRING
            )
            WHEN main_action IN (
                'UNLOCK',
                'CANCEL UNLOCK'
            ) THEN e.instruction :accounts [4] :: STRING
        END AS locker,
        CASE
            WHEN main_action LIKE '% LOCK' THEN COALESCE(
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
            WHEN main_action = 'MINT LOCK' THEN e.instruction :accounts [2] :: STRING
            WHEN main_action IN (
                'UNLOCK',
                'CANCEL UNLOCK'
            ) THEN e.instruction :accounts [1] :: STRING
            ELSE NULL
        END AS locker_nft
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
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    a1.block_timestamp,
    a1.block_id,
    a1.tx_id,
    COALESCE(
        a1.succeeded,
        a2.succeeded
    ) AS succeeded,
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
    ) AS lock_amount
FROM
    actions_tmp a1
    LEFT OUTER JOIN actions_tmp a2
    ON a1.tx_id = a2.tx_id
    AND a1.index <> a2.index
    LEFT OUTER JOIN post_token_balances b
    ON a1.tx_id = b.tx_id
    AND COALESCE(
        a1.locker_account_tmp,
        a2.locker_account_tmp
    ) = b.account
WHERE
    a1.index = a1.min_index
