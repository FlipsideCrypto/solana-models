{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH base AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        tx :transaction :message :recentBlockhash :: STRING AS recent_block_hash,
        tx :meta :fee :: NUMBER AS fee,
        CASE
            WHEN IS_NULL_VALUE(
                tx :meta :err
            ) THEN TRUE
            ELSE FALSE
        END AS succeeded,
        tx :transaction :message :instructions [0] :parsed :info :voteAccount :: STRING AS vote_account,
        tx :transaction :message :instructions [0] :parsed :info :voteAuthority :: STRING AS vote_authority,
        tx :transaction :message :instructions [0] :parsed :info :vote :hash :: STRING AS vote_hash,
        tx :transaction :message :instructions [0] :parsed :info :vote :slots :: ARRAY AS vote_slots,
        ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('bronze__transactions') }}
        t
    WHERE
        tx :transaction :message :instructions [0] :parsed :type :: STRING IS NOT NULL
        AND tx :transaction :message :instructions [0] :programId :: STRING = 'Vote111111111111111111111111111111111111111'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    recent_block_hash,
    fee,
    succeeded,
    vote_account,
    vote_authority,
    vote_hash,
    vote_slots,
    ingested_at,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
