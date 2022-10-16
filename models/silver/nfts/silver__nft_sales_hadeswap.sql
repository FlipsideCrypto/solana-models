{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, mint)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
  block_timestamp,
  block_id,
  tx_id,
  e.index, 
  succeeded,
  program_id,
  INSTRUCTION :accounts [4] :: STRING AS mint,
  INSTRUCTION :accounts [5] :: STRING AS purchaser,
  INSTRUCTION :accounts [3] :: STRING AS seller,
  i.value :parsed :info :lamports / POW(
    10,
    9
  ) AS sales_amount,
  _inserted_timestamp
FROM
  {{ ref('silver__events') }}
  e
  LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
  program_id = 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'
  AND block_timestamp >= '2022-09-22'
  AND VALUE :parsed :type :: STRING = 'transfer'
  AND VALUE :parsed :info :destination :: STRING = INSTRUCTION :accounts [3] :: STRING
  AND succeeded = 'TRUE'
  AND INSTRUCTION :accounts [3] :: STRING != '11111111111111111111111111111111'
  AND INSTRUCTION :accounts [4] :: STRING != '11111111111111111111111111111111'
  AND INSTRUCTION :accounts [5] :: STRING != '11111111111111111111111111111111'

{% if is_incremental() %}
AND
  block_timestamp >= (
    SELECT
      MAX(block_timestamp)
    FROM
      {{ this }}
  )
{% endif %}
