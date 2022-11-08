{{ config(
  materialized = 'incremental',
  unique_key = ['block_id','tx_id','index'],
  merge_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from silver.events__dbt_tmp))'],
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE','program_id'],
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
    _inserted_timestamp
  FROM {{ ref('silver___instructions') }} i

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    i.block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+1,153013616)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+4000000,153013616)
        FROM
            {{ this }}
        ) 
{% elif is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    i.block_id between 105368 and 1000000
{% endif %}
), 

base_ii AS (
  SELECT
    block_id, 
    tx_id, 
    mapped_instruction_index :: INTEGER AS mapped_instruction_index, 
    value,  
    silver.udf_get_all_inner_instruction_program_ids(value) as inner_instruction_program_ids,
    _inserted_timestamp
  FROM {{ ref('silver___inner_instructions') }} ii

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    ii.block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+1,153013616)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+4000000,153013616)
        FROM
            {{ this }}
        ) 
{% elif is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    ii.block_id between 105368 and 1000000
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
  ii.inner_instruction_program_ids,
  i._inserted_timestamp
FROM
  base_i
  i
  LEFT OUTER JOIN base_ii
  ii
  ON ii.block_id = i.block_id
  AND ii.tx_id = i.tx_id
  AND ii.mapped_instruction_index = i.index