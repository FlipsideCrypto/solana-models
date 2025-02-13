-- depends_on: {{ ref('streamline__complete_block_txs_2') }}

{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','block_id','_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id)'),
    full_refresh = false,
    tags = ['scheduled_core']
) }}

{% set cutover_block_id = 307103862 %}
{% set cutover_partition_id = 150215 %}

WITH pre_final AS (
    SELECT
        COALESCE(TO_TIMESTAMP_NTZ(t.value :block_time), b.block_timestamp) AS block_timestamp,
        t.block_id,
        t.tx_id,
        t.data :transaction :message :recentBlockhash :: STRING AS recent_block_hash,
        t.data :meta :fee :: NUMBER AS fee,
        CASE
            WHEN IS_NULL_VALUE(
                t.data :meta :err
            ) THEN TRUE
            ELSE FALSE
        END AS succeeded,
        t.data :transaction :message :accountKeys :: ARRAY AS account_keys,
        t.data :transaction :message :instructions [0] :parsed :info :voteAccount :: STRING AS vote_account,
        t.data :transaction :message :instructions [0] :parsed :info :voteAuthority :: STRING AS vote_authority,
        t.data :transaction :message :instructions [0] :parsed :info :vote :hash :: STRING AS vote_hash,
        t.data :transaction :message :instructions [0] :parsed :info :vote :slots :: ARRAY AS vote_slots,
        t._partition_id,
        t._inserted_timestamp
    FROM
        {{ ref('bronze__transactions2') }} t 
    LEFT OUTER JOIN 
        {{ ref('silver__blocks') }} b on b.block_id = t.block_id
    WHERE
        t.block_id < {{cutover_block_id}}
        AND tx_id is not null
        AND coalesce(
            t.data :transaction :message :instructions [0] :programId :: STRING,
            ''
        ) = 'Vote111111111111111111111111111111111111111'
        {% if is_incremental() %}
        AND _partition_id >= (select max(_partition_id)-1 from {{this}})
        AND _partition_id <= (SELECT MAX(_partition_id) FROM {{ source('solana_streamline','complete_block_txs') }})
        AND t._inserted_timestamp > (select max(_inserted_timestamp) from {{this}})
        {% else %}
        AND _partition_id in (1,2)
        {% endif %}
        AND _partition_id < {{cutover_partition_id}}
    UNION ALL
    SELECT
        t.block_timestamp,
        t.block_id,
        t.data:transaction:signatures[0]::string AS tx_id,
        t.data :transaction :message :recentBlockhash :: STRING AS recent_block_hash,
        t.data :meta :fee :: NUMBER AS fee,
        CASE
            WHEN IS_NULL_VALUE(
                t.data :meta :err
            ) THEN TRUE
            ELSE FALSE
        END AS succeeded,
        t.data :transaction :message :accountKeys :: ARRAY AS account_keys,
        t.data :transaction :message :instructions [0] :parsed :info :voteAccount :: STRING AS vote_account,
        t.data :transaction :message :instructions [0] :parsed :info :voteAuthority :: STRING AS vote_authority,
        t.data :transaction :message :instructions [0] :parsed :info :vote :hash :: STRING AS vote_hash,
        t.data :transaction :message :instructions [0] :parsed :info :vote :slots :: ARRAY AS vote_slots,
        t._partition_id,
        t._inserted_timestamp
    FROM
        {{ ref('bronze__stage_block_txs_2') }} AS t
    WHERE
        t.block_id >= {{ cutover_block_id }}
        AND tx_id IS NOT NULL
        AND coalesce(
            t.data :transaction :message :instructions [0] :programId :: STRING,
            ''
        ) = 'Vote111111111111111111111111111111111111111'
        {% if is_incremental() %}
        AND t._partition_id >= (SELECT max(_partition_id)-1 FROM {{ this }})
        AND t._partition_id <= (SELECT max(_partition_id) FROM {{ ref('streamline__complete_block_txs_2') }})
        AND t._inserted_timestamp > (SELECT max(_inserted_timestamp) FROM {{ this }})
        {% else %}
        AND t._partition_id < 0 /* keep this here, if we ever do a full refresh this should select no data from streamline 2.0 data */
        {% endif %}
        AND t._partition_id >= {{ cutover_partition_id }}
)
{% if is_incremental() %}
, prev_null_block_timestamp_txs as (
    select 
        b.block_timestamp,
        t.block_id,
        t.tx_id,
        t.recent_block_hash,
        t.signers,
        t.fee,
        t.succeeded,
        t.account_keys,
        t.vote_account,
        t.vote_authority,
        t.vote_hash,
        t.vote_slots,
        t._partition_id,
        greatest(t._inserted_timestamp,b._inserted_timestamp) as _inserted_timestamp
    from {{ this }} t
    inner join {{ ref('silver__blocks') }} b on b.block_id = t.block_id
    where t.block_timestamp::date is null
    and t.block_id > 39824213
)
{% endif %}
SELECT
    block_timestamp,
    block_id,
    tx_id,
    recent_block_hash,
    silver.udf_ordered_signers(account_keys) AS signers,
    fee,
    succeeded,
    account_keys,
    vote_account,
    vote_authority,
    vote_hash,
    vote_slots,
    _partition_id,
    _inserted_timestamp
FROM
    pre_final b 
qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
{% if is_incremental() %}
union
select *
from prev_null_block_timestamp_txs
{% endif %}