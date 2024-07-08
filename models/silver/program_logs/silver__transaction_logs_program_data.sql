{{ config(
    materialized = 'incremental',
    unique_key = "transaction_logs_program_data_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, program_id)'),
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_core'],
) }}

/*
Adjusted backfill groupings on 2-XL
2022-08-12	7885521
2022-08-13	11137387
2022-08-15	351101483
2022-08-16	1427863755

2022-08-17	973393045

2022-08-18	827193734

2022-08-19	2346654908

2022-08-20	1281709341

2022-08-21	341131725
2022-08-22	13564732
2022-08-25	384899
2022-08-30	227067618

2022-08-31	2545287850

2022-09-01	1726232252
*/


WITH base AS (
    SELECT 
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        silver.udf_get_logs_program_data(log_messages) AS program_data_logs,
        _inserted_timestamp
    FROM 
        {{ ref('silver__transactions') }}
    WHERE 
        succeeded
        AND log_messages IS NOT NULL
        {% if is_incremental() %}
            /*{% if execute %}
            {{ get_batch_load_logic(this, 30, '2024-07-04') }}
            {% endif %}*/
            AND _inserted_timestamp >= (SELECT max(_inserted_timestamp) FROM {{ this }}) 
        {% else %}
        AND _inserted_timestamp::date BETWEEN '2022-08-12' AND '2022-09-01'
        {% endif %}
)
SELECT 
    t.block_timestamp,
    t.block_id,
    t.tx_id,
    t.succeeded,
    l.value:index::int AS index,
    l.value:inner_index::int AS inner_index,
    l.index AS log_index,
    l.value:program_id::string AS program_id,
    l.value:event_type::string AS event_type,
    l.value:data::string AS data,
    l.value:error::string AS _udf_error,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','index','log_index']
    ) }} AS transaction_logs_program_data_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    base t
JOIN 
    table(flatten(program_data_logs)) l