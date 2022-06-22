{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, mapped_instruction_index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    e.index,
    e.value :index :: NUMBER AS mapped_instruction_index,
    e.value,
    ingested_at,
    _inserted_timestamp
FROM
    {{ ref('silver__transactions') }}
    t,
    TABLE(FLATTEN(inner_instructions)) AS e
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
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
