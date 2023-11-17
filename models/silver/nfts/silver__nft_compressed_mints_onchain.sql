{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  tags = ['compressed_nft']
) }}

WITH bgum_mints AS (

  SELECT
    block_timestamp,
    block_id,
    succeeded,
    tx_id,
    instruction :accounts [1] :: STRING AS leaf_owner,
    instruction :accounts [8] :: STRING AS collection_mint,
    signers [0] :: STRING AS creator_address,
    _inserted_timestamp
  FROM
    {{ ref('silver__events') }}
    e
    INNER JOIN {{ ref('silver__transactions') }}
    txs USING(
      tx_id,
      block_timestamp,
      succeeded
    )
  WHERE
    succeeded
    AND program_id = 'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY'
    AND ARRAY_CONTAINS(
      'Program log: Instruction: MintToCollectionV1' :: variant,
      log_messages
    )
    AND collection_mint IS NOT NULL
    
{% if is_incremental() %}
AND e._inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
AND txs._inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% else %}
  AND block_timestamp :: DATE >= '2023-02-07'
{% endif %}
UNION ALL
SELECT
  block_timestamp,
  block_id,
  succeeded,
  tx_id,
  f.value :accounts [1] :: STRING AS leaf_owner,
  f.value :accounts [8] :: STRING AS collection_mint,
  signers [0] :: STRING AS creator_address,
  _inserted_timestamp
FROM
  {{ ref('silver__events') }}
  e
  INNER JOIN LATERAL FLATTEN (
    input => inner_instruction :instructions
  ) f
  INNER JOIN {{ ref('silver__transactions') }}
  txs USING(
    tx_id,
    block_timestamp,
    succeeded
  )
WHERE
  succeeded
  AND program_id = '1atrmQs3eq1N2FEYWu6tyTXbCjP4uQwExpjtnhXtS8h' -- lazy_transactions
  AND f.value :programId = 'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY' -- bubblegum

{% if is_incremental() %}
AND e._inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
AND txs._inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% else %}
  AND block_timestamp :: DATE >= '2023-02-07'
{% endif %}
)
SELECT
  *
FROM
  bgum_mints
