{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    e.index,
    e.value :parsed :type :: STRING AS event_type,
    e.value,
    ingested_at
FROM
    {{ ref('dbt_solana__transactions') }}
    t,
    TABLE(FLATTEN(instructions)) AS e
WHERE
    COALESCE(
        e.value :programId :: STRING,
        ''
    ) NOT IN (
        'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH',
        'DtmE9D2CSB4L5D6A15mraeEjrGMm6auWVzgaD8hK2tZM'
    )

{% if is_incremental() %}
AND ingested_at >= getdate() - INTERVAL '2 days'
{% endif %}
