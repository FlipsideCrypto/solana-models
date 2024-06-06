{{ config(
    materialized = 'incremental',
    incremental_strategy = "delete+insert",
    incremental_predicates = ['min_value_predicate', 'start_block_id', generate_view_name(this) ~ ".start_block_id >= " ~ generate_tmp_view_name(this) ~ ".start_block_id"],
    unique_key = ["account_address"],
    cluster_by = ["round(start_block_id,-5)"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(account_address, owner)'),
    tags = ['scheduled_non_core']
) }}


WITH new_events AS (
    SELECT
        account_address,
        owner,
        start_block_id,
        _inserted_timestamp,
    FROM
        {{ ref('silver__token_account_owners_intermediate') }}
    WHERE
    {% if is_incremental() %}
        _inserted_timestamp > (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        )
    {% else %}
        _inserted_timestamp :: DATE = '2022-09-01'
    {% endif %}
        AND start_block_id <> coalesce(end_block_id,-1)
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
        {{ ref('silver__token_account_owners_intermediate') }} C
    JOIN 
        distinct_states d
        USING(account_address)
    WHERE
        C.start_block_id >= d.min_block_id
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
        coalesce(lag(owner) OVER (PARTITION BY account_address ORDER BY start_block_id),'abc') <> owner
)
SELECT
    account_address,
    owner,
    start_block_id,
    LEAD(start_block_id) OVER (
        PARTITION BY account_address
        ORDER BY
            start_block_id
    ) AS end_block_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['account_address','start_block_id']) }} AS token_account_owners_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    changed_states
