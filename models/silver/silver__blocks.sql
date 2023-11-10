{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::date'],
  tags = ['scheduled_core']
) }}

{% if is_incremental() %}
WITH last_loaded_timestamp AS (
  SELECT
    COALESCE(MAX(_inserted_timestamp), '2022-08-12T00:00:00' :: timestamp_ntz) AS max_inserted_timestamp
  FROM
    {{ this }}
),
base_blocks AS (
  SELECT
    *
  FROM
    {{ ref('bronze__blocks2') }}
  WHERE
    _inserted_date >= (
      SELECT
        DATEADD(
          'day',
          -1,
          max_inserted_timestamp :: DATE
        )
      FROM
        last_loaded_timestamp
    )
),
next_date_to_load AS (
  SELECT
    MIN(_inserted_timestamp) AS load_timestamp,
    MIN(_inserted_date) AS load_date
  FROM
    base_blocks
  WHERE
    _inserted_timestamp > (
      SELECT
        max_inserted_timestamp
      FROM
        last_loaded_timestamp
    )
),
{% else %}
WITH
{% endif %}
pre_final AS (
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
    {% if is_incremental() %}
      base_blocks
    {% else %}
      {{ ref('bronze__blocks2') }}
    {% endif %}
  WHERE
    block_id IS NOT NULL
    AND error IS NULL

{% if is_incremental() %}
AND _inserted_date = (
  SELECT
    load_date
  FROM
    next_date_to_load
  LIMIT
    1
)
AND _inserted_timestamp >= (
  SELECT
    load_timestamp
  FROM
    next_date_to_load
  LIMIT
    1
)
{% else %}
  AND _inserted_date = '2022-08-12'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_id
ORDER BY
  _inserted_date DESC)) = 1
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
  _inserted_timestamp
FROM
  pre_final
