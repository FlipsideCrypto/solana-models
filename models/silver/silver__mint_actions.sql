{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, event_type, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

WITH base_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
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
    instruction :parsed :info :decimals :: INTEGER AS DECIMAL,
    COALESCE(
        instruction :parsed :info :amount :: INTEGER,
        instruction :parsed :info :tokenAmount: amount :: INTEGER
    ) AS mint_amount,
    _inserted_timestamp
FROM
    base_events
WHERE
    event_type IN (
        'mintTo',
        'initializeMint',
        'mintToChecked',
        'initializeMint2'
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
    i.value :parsed :info :decimals :: INTEGER AS DECIMAL,
    COALESCE(
        i.value :parsed :info :amount :: INTEGER,
        i.value :parsed :info :tokenAmount: amount :: INTEGER
    ) AS mint_amount,
    _inserted_timestamp
FROM
    base_events e,
    TABLE(FLATTEN(inner_instruction :instructions)) i
WHERE
    i.value :parsed :type :: STRING IN (
        'mintTo',
        'initializeMint',
        'mintToChecked',
        'initializeMint2'
    )
