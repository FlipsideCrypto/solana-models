{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['ingested_at::DATE','program_id'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base_i AS (
  SELECT
    block_timestamp, 
    block_id, 
    tx_id, 
    succeeded,
    index :: INTEGER AS index, 
    value:parsed:type AS event_type, 
    value:programId AS program_id, 
    value, 
    ingested_at,
    _inserted_timestamp
  FROM {{ ref('silver___instructions') }} 

{% if is_incremental() %}
  WHERE ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
), 

base_ii AS (
  SELECT
    block_id, 
    tx_id, 
    mapped_instruction_index :: INTEGER AS mapped_instruction_index, 
    value,  
    ingested_at,
    _inserted_timestamp
  FROM {{ ref('silver___inner_instructions') }}

{% if is_incremental() %}
  WHERE ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
) 

SELECT
  i.block_timestamp,
  i.block_id,
  i.tx_id,
  i.succeeded,
  i.index,
  i.event_type :: STRING AS event_type,
  i.program_id :: STRING AS program_id, 
  i.value AS instruction,
  ii.value AS inner_instruction,
  i.ingested_at,
  i._inserted_timestamp
FROM
  base_i
  i
  LEFT OUTER JOIN base_ii
  ii
  ON ii.block_id = i.block_id
  AND ii.tx_id = i.tx_id
  AND ii.mapped_instruction_index = i.index