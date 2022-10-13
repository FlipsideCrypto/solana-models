{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

SELECT 
  block_timestamp,
  block_id,
  tx_id,
  succeeded,
  program_id,
  INSTRUCTION:accounts[4] ::STRING as mint, 
  INSTRUCTION:accounts[5] ::STRING as purchaser, 
  INSTRUCTION:accounts[3] ::STRING as seller, 
  i.value:parsed:info:lamports / POW(10,9) AS sales_amount,
  ingested_at,
  _inserted_timestamp
FROM {{ ref('silver__events') }} e

LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i

WHERE program_id = 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'
  AND value:parsed:type ::STRING = 'transfer'
  AND value:parsed:info:destination ::STRING = INSTRUCTION:accounts[3] ::STRING
  AND succeeded = 'TRUE'
  AND INSTRUCTION:accounts[3] ::STRING != '11111111111111111111111111111111'
  AND INSTRUCTION:accounts[4] ::STRING != '11111111111111111111111111111111'
  AND INSTRUCTION:accounts[5] ::STRING != '11111111111111111111111111111111'

  {% if is_incremental() %}
    AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) FROM {{ this }})
{% else %}
    AND block_timestamp :: date >= '2021-08-01'
{% endif %}