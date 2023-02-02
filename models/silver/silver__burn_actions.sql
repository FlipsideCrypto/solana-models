{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, event_type, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    full_refresh = false
) }}

WITH base_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 31310775)+1,175418104)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 31310775)+4000000,175418104)
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
    -- block_id between 31310775 and 32310775
    block_timestamp between '2022-11-12' and '2022-12-12'
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    null as inner_index,
    event_type,
    instruction :parsed :info :mint :: STRING AS mint,
    COALESCE(
        instruction :parsed :info :amount :: INTEGER,
        instruction :parsed :info :tokenAmount: amount :: INTEGER
    ) AS burn_amount,
    COALESCE(
        instruction :parsed :info :authority :: string,
        instruction :parsed :info :multisigAuthority :: string
    ) AS burn_authority,
    instruction :parsed :info :signers[0] :: string AS signer,
    _inserted_timestamp
FROM
    base_events
WHERE
    event_type IN (
       'burn',
        'burnChecked'
    )
UNION
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    e.index,
    i.index as inner_index,
    i.value :parsed :type :: STRING AS event_type,
    i.value :parsed :info :mint :: STRING AS mint,
    COALESCE(
        i.value :parsed :info :amount :: INTEGER,
        i.value :parsed :info :tokenAmount: amount :: INTEGER
    ) AS burn_amount,
    COALESCE(
        i.value :parsed :info :authority :: string,
        i.value :parsed :info :multisigAuthority :: string
    ) AS burn_authority,
    instruction :parsed :info :signers[0] :: string AS signer,
    _inserted_timestamp
FROM
    base_events e,
    TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :type :: STRING IN (
       'burn',
        'burnChecked'
    )
