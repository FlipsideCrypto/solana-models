{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','tx_id','index'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    full_refresh = false,
    tags = ['scheduled_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    e.index,
    e.value,
    _inserted_timestamp
FROM
    {{ ref('silver__transactions') }}
    t,
    TABLE(FLATTEN(instructions)) AS e

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    t.block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+1,151386092)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368)+4000000,151386092)
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
    t.block_id between 105368 and 1000000
{% endif %}
