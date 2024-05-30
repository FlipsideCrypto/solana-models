{{ config(
    materialized = 'incremental',
    incremental_strategy = "delete+insert",
    incremental_predicates = [generate_view_name(this) ~ ".start_block_id >= " ~ generate_tmp_view_name(this) ~ ".start_block_id"],
    unique_key = ["account_address"],
    cluster_by = ["account_address", "start_block_id"],
    tags = ['scheduled_non_core']
) }}

WITH new_events AS (
    SELECT
        account_address,
        owner,
        start_block_id,
        _inserted_timestamp,
    FROM
        solana.silver.token_account_owners_intermediate
        /*{{ ref('silver__token_account_owners_intermediate') }}*/
    WHERE
    {% if is_incremental() %}
        _inserted_timestamp > (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        )
        AND _inserted_timestamp < (
            SELECT
                MAX(_inserted_timestamp) + INTERVAL '120 day'
            FROM
                {{ this }}
        )
    {% else %}
        _inserted_timestamp :: DATE = '2022-09-01'
    {% endif %}
        AND start_block_id <> coalesce(end_block_id,-1)
        -- AND account_address IN ('38wX3iF3twRED4E2eyvGDQpxrEyK7ZEpjK53AcnYDa2k',
        -- 'Ff7fDF545jffUYiU3ReAzzP4VfBCAh1yfwThjBz4UxcB',
        -- '9HBBbt3PSGdfeGfbTv6XHXqBUv5jk3cfz4kFFn8vJGFL',
        -- '4geDNAvCnE1tLogZnfLYkkbwSENfqxfWh2Cx79Nct9eq',
        -- '9Lf8YK6H28g5Rut3XjPSn5kQRwGqcUYRK5FjThvPxLrj',
        -- 'ArhaTaKiMCiBMwGSxCKMy8V6guioB7N1c8R8B3HGb7U3',
        -- '5Afw8nCxWmPHXbxYNZzSUUWjQiaKe1mPGbNyYYi8U1vN')
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
events_to_reprocess AS (
    SELECT
        C.account_address,
        C.owner,
        C.start_block_id,
        C._inserted_timestamp,
    FROM
        solana.silver.token_account_owners_intermediate C
        /*{{ ref('silver__token_account_owners_intermediate') }}*/
    JOIN 
        distinct_states d
        USING(account_address)
    WHERE
        /*(
            C.end_block_id >= d.min_block_id
            OR 
            C.end_block_id IS NULL 
        )
        AND start_block_id <> coalesce(end_block_id,-1)
        AND _inserted_timestamp < (SELECT max(_inserted_timestamp) FROM new_events)*/
        (
            C.start_block_id >= d.min_block_id
            -- OR 
            -- C.end_block_id IS NULL 
        )
        AND start_block_id <> coalesce(end_block_id,-1)
        AND _inserted_timestamp <= (SELECT max(_inserted_timestamp) FROM new_events)
),
current_state AS (
    select 
        C.account_address,
        C.owner,
        C.start_block_id,
        C._inserted_timestamp,
    from 
        {{ this }} C
    JOIN 
        distinct_states d
        USING(account_address)
    WHERE
        (
            C.end_block_id >= d.min_block_id
            OR 
            C.end_block_id IS NULL 
        )
    QUALIFY
        row_number() over (partition by account_address order by start_block_id) = 1
        
),
{% endif %}
all_states AS (
    {% if is_incremental() %}
    SELECT
        *
    FROM
        current_state
    UNION ALL
    SELECT 
        *
    FROM 
        events_to_reprocess
    {% else %}
    SELECT 
        *
    FROM 
        new_events
    {% endif %}
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
    start_block_id,
    LEAD(start_block_id) over (
        PARTITION BY account_address
        ORDER BY
            start_block_id
    ) AS end_block_id,
    _inserted_timestamp,
FROM
    changed_states
