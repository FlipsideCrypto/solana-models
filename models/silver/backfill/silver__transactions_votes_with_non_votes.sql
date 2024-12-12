{{
    config(
        materialized="incremental",
        tags=['transactions_votes_with_non_votes_backfill']
    )
}}

{% if execute %}
    {% set next_partition_query %}
    {% if is_incremental() %}
        SELECT max(_partition_id)+1, max(_partition_id)+5 FROM {{ this }}
    {% else %}
        SELECT 51779, 51779 /* starting with the first partition of 2024 then decide if worth to do the data prior */
    {% endif %}
    {% endset %}

    {% set next_partition = run_query(next_partition_query)[0][0] %}
    {% set next_partition_2 = run_query(next_partition_query)[0][1] %}
{% endif %}

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
        t.data :version :: STRING as version,
        t._partition_id,
        t._inserted_timestamp
    FROM 
        {{ ref('bronze__transactions2') }} t
    LEFT OUTER JOIN 
        {{ ref('silver__blocks') }} b
        ON b.block_id = t.block_id
    WHERE 
        tx_id IS NOT NULL
        AND (
            COALESCE(t.data :transaction :message :instructions [0] :programId :: STRING,'') = 'Vote111111111111111111111111111111111111111'
        )
        AND _partition_id >= {{ next_partition }}
        AND _partition_id <= {{ next_partition_2 }}
),
vote_txs_with_non_vote_instructions AS (
    SELECT
        tx_id,
        array_agg(i.value:programId::string) within group (order by i.index) as program_ids
    FROM 
        pre_final
    JOIN
        table(flatten(instructions)) i
    WHERE
        coalesce(instructions [0] :programId :: STRING,'') = 'Vote111111111111111111111111111111111111111'
        AND i.value:programId::string NOT IN ('Vote111111111111111111111111111111111111111','ComputeBudget111111111111111111111111111111')
    GROUP BY 1
    HAVING array_size(program_ids) > 0
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
    silver.udf_get_compute_units_consumed(log_messages, instructions) as units_consumed,
    silver.udf_get_compute_units_total(log_messages, instructions) as units_limit,
    silver.udf_get_tx_size(account_keys,instructions,version,address_table_lookups,signers) as tx_size,
    version,
    _partition_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id']) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final b 
JOIN
    vote_txs_with_non_vote_instructions
    USING(tx_id)
qualify
    row_number() over(PARTITION BY block_id, tx_id ORDER BY _inserted_timestamp DESC) = 1
UNION ALL
/* add a placeholder so we can continue on next incremental with a new partition 
    since most partitions wont return anything new */
SELECT
    NULL AS block_timestamp,
    NULL AS block_id,
    NULL AS tx_id,
    NULL AS recent_block_hash,
    NULL AS signers,
    NULL AS fee,
    NULL AS succeeded,
    NULL AS account_keys,
    NULL AS pre_balances,
    NULL AS post_balances,
    NULL AS pre_token_balances,
    NULL AS post_token_balances,
    NULL AS instructions,
    NULL AS inner_instructions,
    NULL AS log_messages,
    NULL AS address_table_lookups,
    NULL AS  units_consumed,
    NULL AS  units_limit,
    NULL AS  tx_size,
    NULL AS version,
    {{ next_partition_2 }} AS _partition_id,
    NULL AS _inserted_timestamp,
    '{{ invocation_id }}' AS transactions_id,
    NULL AS inserted_timestamp,
    NULL AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
