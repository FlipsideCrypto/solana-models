{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::date'],
  tags = ['scheduled_core'],
  full_refresh = false
) }}

{% set cutover_block_id = 295976124 %}

{% if execute %}
  {% set max_inserted_query %}
  SELECT
      max(_inserted_timestamp) AS _inserted_timestamp
  FROM
    {{ this }}
  {% endset %}
  {% set max_inserted_timestamp = run_query(max_inserted_query).columns[0].values()[0] %}

  {% if is_incremental() %}
  {% set get_dates_to_load_query %}
  WITH base_blocks AS (
    SELECT
      block_id,
      value,
      data,
      error,
      _inserted_date,
      _inserted_timestamp
    FROM
      {{ ref('bronze__blocks2') }}
    WHERE
      _inserted_date >= '{{max_inserted_timestamp}}'::DATE - INTERVAL '1 DAY'
      AND block_id < {{cutover_block_id}}
    UNION ALL
    SELECT
      block_id,
      value,
      data,
      error,
      to_date(_partition_by_created_date, 'YYYY_MM_DD') AS _inserted_date,
      _inserted_timestamp
    FROM
      {{ ref('bronze__streamline_blocks_2')}}
    WHERE
      _inserted_date >= '{{max_inserted_timestamp}}'::DATE - INTERVAL '1 DAY'
      AND block_id >= {{cutover_block_id}}
  )
  SELECT
    min(_inserted_timestamp) AS load_timestamp,
    min(_inserted_date) AS load_date
  FROM
    base_blocks
  WHERE
    _inserted_timestamp > '{{max_inserted_timestamp}}'
  {% endset %}

  {% set get_dates_to_load_columns = run_query(get_dates_to_load_query).columns %}
  {% set load_timestamp = get_dates_to_load_columns[0].values()[0] %}
  {% set load_date = get_dates_to_load_columns[1].values()[0] %}
  {% endif %}
{% endif %}


/*
This CTE (pre_final) combines data from two sources:
1. Legacy data from 'bronze__blocks2' for blocks before the cutover_block_id
2. New data using streamline 2.0 raw data from 'bronze__streamline_blocks_2' for blocks after and including the cutover_block_id
*/
WITH pre_final AS (
  SELECT
    value:block_id::INTEGER AS block_id,
    to_timestamp_ntz(data:blockTime) AS block_timestamp,
    'mainnet' AS network,
    'solana' AS chain_id,
    data:blockHeight AS block_height,
    data:blockhash::STRING AS block_hash,
    data:parentSlot AS previous_block_id,
    data:previousBlockhash::STRING AS previous_block_hash,
    _inserted_date,
    _inserted_timestamp
  FROM
    {{ ref('bronze__blocks2') }}
  WHERE
    block_id < {{cutover_block_id}}
    AND block_id IS NOT NULL
    AND error IS NULL
    {% if is_incremental() %}
    AND _inserted_date = '{{ load_date }}'
    AND _inserted_timestamp >= '{{ load_timestamp }}'
    {% else %}
    AND _inserted_date = '2022-08-12'
    {% endif %}
  UNION ALL
  SELECT
    block_id,
    to_timestamp_ntz(data:result:blockTime) AS block_timestamp,
    'mainnet' AS network,
    'solana' AS chain_id,
    data:result:blockHeight AS block_height,
    data:result:blockhash::STRING AS block_hash,
    data:result:parentSlot AS previous_block_id,
    data:result:previousBlockhash::STRING AS previous_block_hash,
    to_date(_partition_by_created_date, 'YYYY_MM_DD') AS _inserted_date,
    _inserted_timestamp
  FROM
    {{ ref('bronze__streamline_blocks_2')}}
  WHERE
    block_id >= {{cutover_block_id}}
    AND block_id IS NOT NULL
    AND error IS NULL
    AND data:error::STRING IS NULL
    {% if is_incremental() %}
    AND _inserted_date = '{{ load_date }}'
    AND _inserted_timestamp >= '{{ load_timestamp }}'
    {% else %}
    AND _inserted_date = '2022-08-12'
    {% endif %}
)
SELECT
  block_id,
  CASE
    WHEN block_timestamp IS NULL THEN 
      DATEADD('millisecond', 500, LAST_VALUE(block_timestamp) IGNORE NULLS OVER (ORDER BY block_id ROWS UNBOUNDED PRECEDING))
    ELSE 
      block_timestamp
  END AS block_timestamp,
  network,
  chain_id,
  block_height,
  block_hash,
  previous_block_id,
  previous_block_hash,
  _inserted_date,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(['block_id']) }} AS blocks_id,
  sysdate() AS inserted_timestamp,
  sysdate() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  pre_final
QUALIFY
  row_number() OVER (PARTITION BY block_id ORDER BY _inserted_timestamp DESC) = 1
