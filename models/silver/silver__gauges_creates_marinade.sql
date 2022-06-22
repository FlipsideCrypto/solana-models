{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH create_validator_gauge_events AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        instruction,
        inner_instruction
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'va12L6Z9fa5aGJ7gxtJuQZ928nySAk5UetjcGPve3Nu' -- validator gauge creation program id
        AND _inserted_timestamp :: DATE >= '2022-05-17'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
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
        INNER JOIN (
            SELECT
                DISTINCT tx_id
            FROM
                create_validator_gauge_events
        ) g
        ON t.tx_id = g.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l
    WHERE
        _inserted_timestamp :: DATE >= '2022-05-17'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
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
create_validator_logs AS (
    SELECT
        *
    FROM
        tx_logs
    WHERE
        log_message LIKE 'Program log: Instruction: CreateValidatorGauge%'
)
SELECT
    e.block_timestamp,
    e.block_id,
    e.tx_id,
    e.index,
    l.succeeded,
    e.instruction :accounts [0] :: STRING AS signer,
    e.instruction :accounts [3] :: STRING AS gauge,
    e.instruction :accounts [4] :: STRING AS gaugemeister,
    d.data :validator_account :: STRING AS validator_account
FROM
    create_validator_gauge_events e
    INNER JOIN create_validator_logs l
    ON l.tx_id = e.tx_id
    AND e.index = l.event_index
    LEFT OUTER JOIN {{ ref('silver__decoded_instructions_data') }}
    d
    ON e.tx_id = d.tx_id
    AND e.index = d.event_index
    AND d.instruction_type = 'CreateValidatorGauge'
    AND d.program_id = 'va12L6Z9fa5aGJ7gxtJuQZ928nySAk5UetjcGPve3Nu'
WHERE
    -- marinade tribeca programid
    e.inner_instruction :instructions [0] :programId :: STRING = 'tovt1VkTE2T4caWoeFP6a2xSFoew5mNpd7FWidyyMuk'
