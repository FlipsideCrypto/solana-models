{{ config(
    materialized = 'incremental',
    incremental_strategy = "delete+insert",
    incremental_predicates = [generate_view_name(this) ~ ".start_block_id >= " ~ generate_tmp_view_name(this) ~ ".start_block_id"],
    unique_key = ["start_block_id","account_address"],
    cluster_by = ["account_address", "start_block_id"],
    tags = ['scheduled_non_core']
) }}

WITH new_events AS (
    SELECT
        account_address,
        owner,
        -- event_type,
        start_block_id,
        _inserted_timestamp,
    FROM
        {{ ref('silver__token_account_owners_intermediate') }}
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
            MAX(_inserted_timestamp) + INTERVAL '20 day'
        FROM
            {{ this }}
    )
    {% else %}
        _inserted_timestamp :: DATE = '2022-09-01'
    {% endif %}
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
        C.account_address,
        C.owner,
        -- C.event_type,
        C.start_block_id,
        C._inserted_timestamp,
        -- CASE
        --     WHEN C.event_type IN (
        --         'create',
        --         'createIdempotent',
        --         'createAccount',
        --         'createAccountWithSeed'
        --     ) THEN 0
        --     WHEN C.event_type IN (
        --         'initializeAccount',
        --         'initializeAccount2',
        --         'initializeAccount3'
        --     ) THEN 1
        --     ELSE 2
        -- END AS same_block_order_index
    FROM
        {{ this }} C
        JOIN distinct_states d
        USING(account_address)
    WHERE
        (
            C.start_block_id >= d.min_block_id
            OR 
            C.end_block_id IS NULL 
        )
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
),
changed_states AS (
    SELECT 
        *
    FROM 
        all_states 
    QUALIFY 
        coalesce(lag(owner) over (partition by account_address order by start_block_id),'abc') <> owner
)
SELECT
    account_address,
    owner,
    -- event_type,
    start_block_id,
    LEAD(start_block_id) over (
        PARTITION BY account_address
        ORDER BY
            start_block_id
    ) AS end_block_id,
    _inserted_timestamp,
FROM
    changed_states
