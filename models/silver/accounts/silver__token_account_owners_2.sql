{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    unique_key = ["start_block_id","account_address"],
    cluster_by = ["start_block_id","block_timestamp::date"],
    tags = ['scheduled_non_core']
) }}

WITH new_events AS (
    SELECT
        block_timestamp,
        account_address,
        owner,
        event_type,
        block_id AS start_block_id,
        _inserted_timestamp,
        CASE
            WHEN event_type IN (
                'create',
                'createIdempotent',
                'createAccount',
                'createAccountWithSeed'
            ) THEN 0
            WHEN event_type IN (
                'initializeAccount',
                'initializeAccount2',
                'initializeAccount3'
            ) THEN 1
            ELSE 2
        END AS same_block_order_index
    FROM
        {{ ref('silver__token_account_ownership_events') }}
    WHERE
    {% if is_incremental() %}
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    AND _inserted_timestamp < (
        SELECT
            MAX(_inserted_timestamp) + INTERVAL '10 day'
        FROM
            {{ this }}
    )
    {% else %}
        _inserted_timestamp :: DATE = '2022-08-12'
    {% endif %}

    qualify ROW_NUMBER() over (
        PARTITION BY account_address,
        start_block_id
        ORDER BY
            same_block_order_index DESC
    ) = 1
),

{% if is_incremental() %}
distinct_states AS (
    SELECT
        account_address,
        MIN(start_block_id) AS min_block_id
    FROM
        new_events
    GROUP BY
        1
),
current_state AS (
    SELECT
        C.block_timestamp,
        C.account_address,
        C.owner,
        C.event_type,
        C.start_block_id,
        C._inserted_timestamp,
        CASE
            WHEN C.event_type IN (
                'create',
                'createIdempotent',
                'createAccount',
                'createAccountWithSeed'
            ) THEN 0
            WHEN C.event_type IN (
                'initializeAccount',
                'initializeAccount2',
                'initializeAccount3'
            ) THEN 1
            ELSE 2
        END AS same_block_order_index
    FROM
        {{ this }} C
        JOIN distinct_states d
        ON (
            C.start_block_id < d.min_block_id
        )
        AND C.account_address = d.account_address
),
{% endif %}
all_states AS (
    {% if is_incremental() %}
    SELECT
        *
    FROM
        current_state
    UNION ALL
    {% endif %}
    SELECT
        *
    FROM
        new_events
)
SELECT
    block_timestamp,
    account_address,
    owner,
    event_type,
    start_block_id,
    LEAD(start_block_id) over (
        PARTITION BY account_address
        ORDER BY
            block_timestamp
    ) AS end_block_id,
    _inserted_timestamp,
FROM
    all_states
