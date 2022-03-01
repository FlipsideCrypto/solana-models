{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base_i AS (
  SELECT
    block_timestamp, 
    block_id, 
    tx_id, 
    index :: INTEGER AS index, 
    value:parsed:type AS event_type, 
    value:programId AS program_id, 
    value, 
    ingested_at
  FROM {{ ref('silver___instructions') }} 

{% if is_incremental() %}
  WHERE ingested_at >= getdate() - interval '2 days'
{% endif %}
), 

base_ii AS (
  SELECT
    block_id, 
    tx_id, 
    mapped_instruction_index :: INTEGER AS mapped_instruction_index, 
    value,  
    ingested_at
  FROM {{ ref('silver___inner_instructions') }}

{% if is_incremental() %}
  WHERE ingested_at >= getdate() - interval '2 days'
{% endif %}
) 

SELECT
  i.block_timestamp,
  i.block_id :: INTEGER AS block_id,
  i.tx_id :: STRING AS tx_id,
  i.index :: INTEGER AS INDEX,
  i.event_type :: STRING AS event_type,
  i.program_id :: STRING AS program_id, 
  i.value AS instruction,
  ii.value AS inner_instruction,
  i.ingested_at
FROM
  base_i
  i
  LEFT OUTER JOIN base_ii
  ii
  ON ii.block_id = i.block_id
  AND ii.tx_id = i.tx_id
  AND ii.mapped_instruction_index = i.index