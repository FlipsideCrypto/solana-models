{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'VALIDATOR' }}},
    unique_key = ['fact_validators_id'],
    cluster_by = ['epoch','epoch_active'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(node_pubkey, vote_pubkey)'),
    tags = ['scheduled_non_core'],
    full_refresh = false,
) }}

{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(modified_timestamp) AS max_modified_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

SELECT
  epoch_recorded :: INT AS epoch,
  node_pubkey,
  vote_pubkey,
  active_stake,
  admin_warning,
  avatar_url,
  commission,
  created_at,
  data_center_host,
  data_center_key,
  delinquent,
  details,
  epoch_active,
  epoch_credits,
  keybase_id,
  latitude,
  longitude,
  validator_name,
  software_version,
  updated_at,
  www_url,
  COALESCE (
    snapshot_validators_app_data_id,
    {{ dbt_utils.generate_surrogate_key(
      ['epoch', 'node_pubkey']
    ) }}
  ) AS fact_validators_id,
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp
FROM
  {{ ref('silver__snapshot_validators_app_data') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
-- historical data -- tables static and disabled, and manual change needed in rare case where fr is needed
{# 
{% if not is_incremental() %}
UNION ALL
SELECT
  epoch_recorded :: INT AS epoch,
  node_pubkey,
  vote_pubkey,
  active_stake,
  admin_warning,
  avatar_url,
  commission,
  created_at,
  data_center_host,
  data_center_key,
  delinquent,
  details,
  epoch_active,
  epoch_credits,
  keybase_id,
  latitude,
  longitude,
  validator_name,
  software_version,
  updated_at,
  www_url,
  {{ dbt_utils.generate_surrogate_key(
    ['epoch', 'node_pubkey']
  ) }} AS fact_validators_id,
  '2000-01-01' AS inserted_timestamp,
  '2000-01-01' AS modified_timestamp
FROM
  {{ ref('silver__historical_validator_app_data') }}
{% endif %}
#}
