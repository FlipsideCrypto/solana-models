{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, mint)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

WITH mint as (

SELECT 
  e.tx_id, 
  e.index,
  i.value:parsed:info:mint ::STRING as mint
FROM {{ ref('silver__events') }} e

LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i

WHERE program_id = 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'
  AND value:parsed:type ::STRING = 'create'
  AND SUCCEEDED = 'TRUE'
  AND block_timestamp >= '2022-09-22'

{% if is_incremental() %}
  AND block_timestamp >= (SELECT MAX(block_timestamp) FROM {{ this }})
{% endif %}  

)

SELECT 
  block_timestamp,
  block_id,
  e.tx_id,
  e.index,
  succeeded,
  program_id,
  m.mint, 
  i.value:parsed:info:source ::STRING as purchaser, 
  i.value:parsed:info:destination ::STRING as seller, 
  i.value:parsed:info:lamports / POW(10,9) AS sales_amount,
  _inserted_timestamp
FROM {{ ref('silver__events') }} e

LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i

LEFT OUTER JOIN mint m
  ON e.tx_id = m.tx_id
  AND e.index = m.index

WHERE program_id = 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'
  AND block_timestamp >= '2022-09-22'
  AND value:parsed:type ::STRING = 'transfer'
  AND value:parsed:info:destination ::STRING = INSTRUCTION:accounts[3] ::STRING
  AND succeeded = 'TRUE'
  AND INSTRUCTION:accounts[3] ::STRING != '11111111111111111111111111111111'
  AND INSTRUCTION:accounts[4] ::STRING != '11111111111111111111111111111111'
  AND INSTRUCTION:accounts[5] ::STRING != '11111111111111111111111111111111'
  
{% if is_incremental() %}
  AND block_timestamp >= (SELECT MAX(block_timestamp) FROM {{ this }})
{% endif %}  
