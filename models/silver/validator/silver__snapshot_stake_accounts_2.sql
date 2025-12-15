{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ["epoch_recorded", "stake_pubkey"],
    cluster_by = ['modified_timestamp::DATE', '_inserted_timestamp::DATE'],
    full_refresh = false,
    tags = ['scheduled_non_core_hourly']
) }}

WITH base AS (
    SELECT 
        group_num,
        invocation_id,
        split(replace(replace(replace(ACCOUNTS_REQUESTED,'"',''),'[',''),']',''),',') AS accounts_requested,
        data,
        _partition_by_created_date,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_stake_program_accounts_2') }} 
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= (SELECT max(_inserted_timestamp) FROM {{ this }})
        AND _partition_by_created_date >= replace((current_date - 4)::string,'-','_') -- default lookback buffer
    {% endif %}
),
responses_flattened AS (
    SELECT
        group_num,
        invocation_id,
        data:result:context:slot::number as slot_recorded,
        r.value::variant AS account_info,
        r.index,
        _partition_by_created_date,
        _inserted_timestamp
    FROM
        base
    JOIN 
        table(flatten(data:result:value)) AS r
),
accounts_requested_flattened AS (
    SELECT
        group_num,
        invocation_id,
        a.value::string AS account_address,
        a.index,
        _partition_by_created_date,
        _inserted_timestamp
    FROM 
        base 
    JOIN 
        table(flatten(accounts_requested)) AS a
),
stake_program_accounts AS (
    SELECT
        a.account_address,
        a.index,
        r.slot_recorded,
        r.account_info,
        a.group_num,
        a.invocation_id,
        a._partition_by_created_date,
        a._inserted_timestamp
    FROM
        accounts_requested_flattened a
    LEFT JOIN
        responses_flattened r
        ON a._partition_by_created_date = r._partition_by_created_date
        AND a.group_num = r.group_num
        AND a.invocation_id = r.invocation_id
        AND a.index = r.index
),
parsed_account_info AS (
    SELECT
        account_address AS stake_pubkey,
        slot_recorded,
        account_info:data:parsed:info:meta:authorized:staker::string AS authorized_staker,
        account_info:data:parsed:info:meta:authorized:withdrawer::string AS authorized_withdrawer,
        account_info:data:parsed:info:meta:lockup::object AS lockup,
        account_info:data:parsed:info:meta:rentExemptReserve::number AS rent_exempt_reserve,
        account_info:data:parsed:info:stake:creditsObserved::number AS credits_observed,
        account_info:data:parsed:info:stake:delegation:activationEpoch::number AS activation_epoch,
        account_info:data:parsed:info:stake:delegation:deactivationEpoch::number AS deactivation_epoch,
        account_info:data:parsed:info:stake:delegation:stake::number / pow(10,9) AS active_stake,
        account_info:data:parsed:info:stake:delegation:voter::string AS vote_pubkey,
        account_info:data:parsed:info:stake:delegation:warmupCooldownRate::number AS warmup_cooldown_rate,
        account_info:data:parsed:type::string AS type_stake,
        account_info:data:program::string AS program,
        account_info:lamports / pow(10,9) AS account_sol,
        account_info:rentEpoch::number AS rent_epoch,
        invocation_id,
        _inserted_timestamp
    FROM
        stake_program_accounts
    WHERE 
        account_info::string IS NOT NULL
),
epochs_recorded AS (
    SELECT
        distinct(a.slot_recorded),
        b.epoch
    FROM
        parsed_account_info a
    left join {{ ref('silver__epoch') }} b
    on a.slot_recorded between b.start_block and b.end_block
),

-- Find the earliest _inserted_timestamp (rounded to hour) for each epoch
earliest_ingestion_per_epoch AS (
    SELECT
        e.epoch,
        MIN(DATE_TRUNC('hour', a._inserted_timestamp)) AS earliest_ingestion_hour
    FROM
        parsed_account_info a
    LEFT JOIN epochs_recorded e
        ON a.slot_recorded = e.slot_recorded
    WHERE e.epoch IS NOT NULL
    GROUP BY e.epoch
),

-- Filter to only include records from the earliest ingestion hour per epoch
filtered_account_info AS (
    SELECT
        a.*,
        e.epoch
    FROM
        parsed_account_info a
    LEFT JOIN epochs_recorded e
        ON a.slot_recorded = e.slot_recorded
    INNER JOIN earliest_ingestion_per_epoch ei
        ON e.epoch = ei.epoch
        AND DATE_TRUNC('hour', a._inserted_timestamp) = ei.earliest_ingestion_hour
    WHERE e.epoch IS NOT NULL
),
pre_final AS (
    SELECT
        stake_pubkey,
        authorized_staker,
        authorized_withdrawer,
        lockup,
        rent_exempt_reserve,
        credits_observed,
        activation_epoch,
        deactivation_epoch,
        active_stake,
        vote_pubkey,
        warmup_cooldown_rate,
        type_stake,
        program,
        account_sol,
        rent_epoch,
        epoch AS epoch_recorded,
        slot_recorded,
        _inserted_timestamp
    FROM
        filtered_account_info
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['epoch_recorded', 'stake_pubkey']
    ) }} AS snapshot_stake_accounts_2_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final 
QUALIFY
    row_number() OVER (PARTITION BY epoch_recorded, stake_pubkey ORDER BY _inserted_timestamp DESC) = 1
