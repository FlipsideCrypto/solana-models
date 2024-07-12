{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::date'],
  tags = ['scheduled_core'],
  full_refresh = false
) }}

{% if execute %}
  {% set max_inserted_query %}
  SELECT
      MAX(_inserted_timestamp) AS _inserted_timestamp
  FROM
    {{ this }}
  {% endset %}
  {% set max_inserted_timestamp = run_query(max_inserted_query).columns[0].values()[0] %}

  {% if is_incremental() %}
  {% set get_dates_to_load_query %}
    WITH base_blocks AS (
      SELECT
        *
      FROM
        {{ ref('bronze__blocks2') }}
      WHERE
        _inserted_date >= '{{max_inserted_timestamp}}'::date - INTERVAL '1 DAY'
    )
    SELECT
      MIN(_inserted_timestamp) AS load_timestamp,
      MIN(_inserted_date) AS load_date
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


WITH pre_final AS (
  SELECT
    VALUE :block_id :: INTEGER AS block_id,
    TO_TIMESTAMP_NTZ(
      DATA :blockTime
    ) AS block_timestamp,
    'mainnet' AS network,
    'solana' AS chain_id,
    DATA :blockHeight AS block_height,
    DATA :blockhash :: STRING AS block_hash,
    DATA :parentSlot AS previous_block_id,
    DATA :previousBlockhash :: STRING AS previous_block_hash,
    _inserted_date,
    _inserted_timestamp
  FROM
    {{ ref('bronze__blocks2') }}
  WHERE
    block_id IS NOT NULL
    AND error IS NULL
    {% if is_incremental() %}
    AND _inserted_date = '{{load_date}}'
    AND _inserted_timestamp >= '{{load_timestamp}}'
    {% else %}
    AND _inserted_date = '2022-08-12'
    {% endif %}
  qualify(ROW_NUMBER() over(PARTITION BY block_id ORDER BY _inserted_date DESC)) = 1
)
SELECT
  block_id,
  CASE
    WHEN block_timestamp IS NULL THEN DATEADD('millisecond', 500, LAST_VALUE(block_timestamp) ignore nulls over (
    ORDER BY
      block_id rows unbounded preceding))
      ELSE block_timestamp
  END AS block_timestamp,
  network,
  chain_id,
  block_height,
  block_hash,
  previous_block_id,
  previous_block_hash,
  _inserted_date,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS blocks_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  pre_final
