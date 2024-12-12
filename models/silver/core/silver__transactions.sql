-- depends_on: {{ ref('streamline__complete_block_txs_2') }}

{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','block_id','_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id)'),
    full_refresh = false,
    merge_exclude_columns = ["inserted_timestamp"],
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
        t.data :meta :preBalances :: ARRAY AS pre_balances,
        t.data :meta :postBalances :: ARRAY AS post_balances,
        t.data :meta :preTokenBalances :: ARRAY AS pre_token_balances,
        t.data :meta :postTokenBalances :: ARRAY AS post_token_balances,
        t.data :transaction :message :instructions :: ARRAY AS instructions,
        t.data :meta :innerInstructions :: ARRAY AS inner_instructions,
        t.data :meta :logMessages :: ARRAY AS log_messages,
        t.data:transaction:message:addressTableLookups::array as address_table_lookups,
        t.data :meta :computeUnitsConsumed :: NUMBER as compute_units_consumed,
        t.data :version :: STRING as version,
        t._partition_id,
        t._inserted_timestamp
    FROM
        {{ ref('bronze__transactions2') }} AS t
        LEFT OUTER JOIN {{ ref('silver__blocks') }} AS b
        ON b.block_id = t.block_id
    WHERE
        t.block_id < {{cutover_block_id}}
        AND tx_id IS NOT NULL
        AND (
            COALESCE(t.data :transaction :message :instructions [0] :programId :: STRING,'') <> 'Vote111111111111111111111111111111111111111'
            OR
            (
                array_size(t.data :transaction :message :instructions) > 1
            )
        )
        {% if is_incremental() %}
        AND _partition_id >= (SELECT max(_partition_id)-1 FROM {{ this }})
        AND _partition_id <= (SELECT max(_partition_id) FROM {{ source('solana_streamline','complete_block_txs') }})
        AND t._inserted_timestamp > (SELECT max(_inserted_timestamp) FROM {{ this }})
        {% else %}
        AND _partition_id IN (1,2)
        {% endif %}
        AND _partition_id < {{cutover_partition_id}}
    UNION ALL
    SELECT
        to_timestamp_ntz(t.value:"result.blockTime"::int) AS block_timestamp,
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
        t.data :meta :preBalances :: ARRAY AS pre_balances,
        t.data :meta :postBalances :: ARRAY AS post_balances,
        t.data :meta :preTokenBalances :: ARRAY AS pre_token_balances,
        t.data :meta :postTokenBalances :: ARRAY AS post_token_balances,
        t.data :transaction :message :instructions :: ARRAY AS instructions,
        t.data :meta :innerInstructions :: ARRAY AS inner_instructions,
        t.data :meta :logMessages :: ARRAY AS log_messages,
        t.data:transaction:message:addressTableLookups::array as address_table_lookups,
        t.data :meta :computeUnitsConsumed :: NUMBER as compute_units_consumed,
        t.data :version :: STRING as version,
        t._partition_id,
        t._inserted_timestamp
    FROM
        {{ ref('bronze__streamline_block_txs_2') }} AS t
    WHERE
        t.block_id >= {{ cutover_block_id }}
        AND tx_id IS NOT NULL
        AND (
            COALESCE(t.data :transaction :message :instructions [0] :programId :: STRING,'') <> 'Vote111111111111111111111111111111111111111'
            OR
            (
                array_size(t.data :transaction :message :instructions) > 1
            )
        )
        {% if is_incremental() %}
        AND t._partition_id >= (SELECT max(_partition_id)-1 FROM {{ this }})
        AND t._partition_id <= (SELECT max(_partition_id) FROM {{ ref('streamline__complete_block_txs_2') }})
        AND t._inserted_timestamp > (SELECT max(_inserted_timestamp) FROM {{ this }})
        {% else %}
        AND t._partition_id < 0 /* keep this here, if we ever do a full refresh this should select no data from streamline 2.0 data */
        {% endif %}
        AND t._partition_id >= {{ cutover_partition_id }}
),
{% if is_incremental() %}
prev_null_block_timestamp_txs AS (
    SELECT
        b.block_timestamp,
        t.block_id,
        t.tx_id,
        t.recent_block_hash,
        t.signers,
        t.fee,
        t.succeeded,
        t.account_keys,
        t.pre_balances,
        t.post_balances,
        t.pre_token_balances,
        t.post_token_balances,
        t.instructions,
        t.inner_instructions,
        t.log_messages,
        t.address_table_lookups,
        t.units_consumed,
        t.units_limit,
        t.tx_size,
        t.version,
        t._partition_id,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__blocks') }}
        b
        ON b.block_id = t.block_id
    WHERE
        t.block_timestamp :: DATE IS NULL
        AND t.block_id > 39824213
),
{% endif %}
qualifying_transactions AS (
    SELECT
        tx_id,
        array_agg(i.value:programId::string) within group (order by i.index) as program_ids
    FROM 
        pre_final
    JOIN
        table(flatten(instructions)) i
    WHERE
        (
        coalesce(instructions [0] :programId :: STRING,'') <> 'Vote111111111111111111111111111111111111111'
        OR
        /* small amount of txs have non-compute instructions after the vote */
        i.value:programId::string NOT IN ('Vote111111111111111111111111111111111111111','ComputeBudget111111111111111111111111111111')
    )
    GROUP BY 1
    HAVING array_size(program_ids) > 0
    UNION ALL
    /* some txs have no instructions at all, this is being filtered out above so we need to make sure we grab these */
    SELECT
        tx_id,
        null
    FROM
        pre_final
    WHERE
        array_size(instructions) = 0
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    recent_block_hash,
    silver.udf_ordered_signers(account_keys) AS signers,
    fee,
    succeeded,
    account_keys,
    pre_balances,
    post_balances,
    pre_token_balances,
    post_token_balances,
    instructions,
    inner_instructions,
    log_messages,
    address_table_lookups,
    CASE 
        WHEN block_id > 204777016 THEN compute_units_consumed 
        ELSE silver.udf_get_compute_units_consumed(log_messages, instructions) 
    END AS units_consumed,
    silver.udf_get_compute_units_total(log_messages, instructions) as units_limit,
    silver.udf_get_tx_size(account_keys,instructions,version,address_table_lookups,signers) as tx_size,
    version,
    _partition_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final b 
JOIN
    qualifying_transactions
    USING(tx_id)
qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1

{% if is_incremental() %}
UNION
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    prev_null_block_timestamp_txs
{% endif %}
