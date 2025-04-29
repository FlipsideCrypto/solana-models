{{ 
  config(
    materialized = 'incremental',
    meta = { 'database_tags': { 'table': { 'PURPOSE': 'VALIDATOR' }}},
    unique_key = ['fact_stake_accounts_id'],
    cluster_by = ['epoch', 'activation_epoch', 'deactivation_epoch'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(stake_pubkey, vote_pubkey)'),
    tags = ['scheduled_non_core_hourly']
  ) 
}}

{% set v1_cutoff_epoch = 669 %}

{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT 
              max(modified_timestamp) AS max_modified_timestamp
            FROM 
              {{ this }}
        {% endset %}
        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

SELECT
    epoch_recorded::INT AS epoch,
    stake_pubkey,
    vote_pubkey,
    authorized_staker,
    authorized_withdrawer,
    lockup,
    rent_exempt_reserve,
    credits_observed,
    activation_epoch,
    deactivation_epoch,
    iff(epoch > deactivation_epoch, 0, active_stake) AS active_stake,
    warmup_cooldown_rate,
    type_stake,
    program,
    account_sol,
    rent_epoch,
    snapshot_stake_accounts_2_id AS fact_stake_accounts_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__snapshot_stake_accounts_2') }}
WHERE
    epoch > {{ v1_cutoff_epoch }}
    {% if is_incremental() %}
    AND modified_timestamp >= '{{ max_modified_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    epoch_recorded::INT AS epoch,
    stake_pubkey,
    vote_pubkey,
    authorized_staker,
    authorized_withdrawer,
    lockup,
    rent_exempt_reserve,
    credits_observed,
    activation_epoch,
    deactivation_epoch,
    iff(epoch > deactivation_epoch, 0, active_stake) AS active_stake,
    warmup_cooldown_rate,
    type_stake,
    program,
    account_sol,
    rent_epoch,
    coalesce(snapshot_stake_accounts_id, {{ dbt_utils.generate_surrogate_key(['epoch', 'stake_pubkey']) }}) AS fact_stake_accounts_id,
    coalesce(inserted_timestamp, '2000-01-01') AS inserted_timestamp,
    coalesce(modified_timestamp, '2000-01-01') AS modified_timestamp
FROM
    {{ ref('silver__snapshot_stake_accounts') }}
WHERE
    epoch <= {{ v1_cutoff_epoch }}
    {% if is_incremental() %}
    AND modified_timestamp >= '{{ max_modified_timestamp }}'
    {% endif %}
{% if not is_incremental() %}
UNION ALL
SELECT
    epoch_ingested_at::INT AS epoch,
    stake_pubkey,
    vote_pubkey,
    authorized_staker,
    authorized_withdrawer,
    lockup,
    rent_exempt_reserve,
    credits_observed,
    activation_epoch,
    deactivation_epoch,
    iff(epoch > deactivation_epoch, 0, active_stake) AS active_stake,
    warmup_cooldown_rate,
    type_stake,
    program,
    account_sol,
    rent_epoch,
    {{ dbt_utils.generate_surrogate_key(['epoch', 'stake_pubkey']) }} AS fact_stake_accounts_id,
    '2000-01-01' AS inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__historical_stake_account') }}
{% endif %}
