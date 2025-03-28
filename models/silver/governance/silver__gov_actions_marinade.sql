{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['daily']
) }}

WITH base AS (

    SELECT
        DISTINCT locker_account AS locker_account
    FROM
        {{ ref ('silver__gov_actions_marinade_tmp') }}
),
more_locks AS (
    SELECT
        DISTINCT tx_id
    FROM
        {{ ref ('silver__events') }}
    WHERE
        program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND event_type IN (
            'transfer',
            'transferChecked'
        )
        AND instruction :parsed :info :destination IN (
            SELECT
                locker_account
            FROM
                base
        )
    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                GREATEST(MAX(_inserted_timestamp),current_date-3)
            FROM
                {{ this }}
        )
    {% else %}
    AND
        block_timestamp :: DATE >= '2022-04-01'
    {% endif %}
    EXCEPT
    SELECT
        tx_id
    FROM
        {{ ref ('silver__gov_actions_marinade_tmp') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                GREATEST(MAX(_inserted_timestamp),current_date-3)
            FROM
                {{ this }}
        )
    {% endif %}
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    signer,
    locker_account,
    locker_nft,
    mint,
    action,
    amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS gov_actions_marinade_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref ('silver__gov_actions_marinade_tmp') }}
WHERE
    succeeded
    AND signer IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        GREATEST(MAX(_inserted_timestamp),current_date-3)
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    e.block_id,
    e.block_timestamp,
    e.tx_id,
    t.succeeded,
    e.instruction :parsed :info :authority :: STRING AS signer,
    e.instruction :parsed :info :destination :: STRING AS locker_account,
    NULL AS locker_nft,
    'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey' AS mint,
    'UPDATE LOCK' AS action,
    (
        COALESCE(
            e.instruction :parsed :info :tokenAmount :amount,
            e.instruction :parsed :info :amount
        ) * pow(
            10,
            -9
        )
    ) :: FLOAT AS amount,
    e._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['e.tx_id']
    ) }} AS gov_actions_marinade_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref ('silver__events') }}
    e
    INNER JOIN more_locks ml
    ON ml.tx_id = e.tx_id
    INNER JOIN {{ ref ('silver__transactions') }}
    t
    ON t.tx_id = e.tx_id

{% if is_incremental() %}
WHERE
    e._inserted_timestamp >= (
        SELECT
            GREATEST(MAX(_inserted_timestamp),current_date-3)
        FROM
            {{ this }}
    )
    AND t._inserted_timestamp >= (
        SELECT
            GREATEST(MAX(_inserted_timestamp),current_date-3)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    e.block_timestamp :: DATE >= '2022-04-01'
    AND t.block_timestamp :: DATE >= '2022-04-01'
{% endif %}
