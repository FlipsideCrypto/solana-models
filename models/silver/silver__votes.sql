{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
) }}

WITH base AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        tx :transaction :message :recentBlockhash :: STRING AS recent_block_hash,
        tx :meta :fee :: NUMBER AS fee,
        CASE
            WHEN tx :meta :err IS NULL THEN TRUE
            ELSE FALSE
        END AS succeeded,
        tx :transaction :message :instructions [0] :parsed :info :voteAccount :: STRING AS vote_account,
        tx :transaction :message :instructions [0] :parsed :info :voteAuthority :: STRING AS vote_authority,
        tx :transaction :message :instructions [0] :parsed :info :vote :hash :: STRING AS vote_hash,
        tx :transaction :message :instructions [0] :parsed :info :vote :slots :: ARRAY AS vote_slots,
        ingested_at
    FROM
        {{ ref('bronze__transactions') }}
        t
    WHERE
        tx :transaction :message :instructions [0] :parsed :type :: STRING IS NOT NULL
        AND tx :transaction :message :instructions [0] :programId :: STRING = 'Vote111111111111111111111111111111111111111'

{% if is_incremental() %}
AND ingested_at :: DATE >= getdate() - INTERVAL '2 days'
{% endif %}
)
SELECT
    block_timestamp,ß block_id,
    tx_id,
    recent_block_hash,
    fee,
    succeeded,
    vote_account,
    vote_authority,
    vote_hash,
    vote_slots,
    ingested_at
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
    ingested_at DESC)) = 1
