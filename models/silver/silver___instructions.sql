{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    e.index,
    e.value,
    ingested_at
FROM
    {{ ref('silver__transactions') }}
    t,
    TABLE(FLATTEN(instructions)) AS e
WHERE
    COALESCE(
        e.value :programId :: STRING,
        ''
    ) NOT IN (
        -- exclude Pyth Oracle programs
        'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH',
        'DtmE9D2CSB4L5D6A15mraeEjrGMm6auWVzgaD8hK2tZM'
    )

{% if is_incremental() %}
AND ingested_at :: DATE >= getdate() - INTERVAL '2 days'
{% endif %}
