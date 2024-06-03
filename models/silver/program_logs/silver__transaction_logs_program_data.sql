{{ config(
    materialized = 'incremental',
    unique_key = "transaction_logs_program_data_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id)'),
    merge_exclude_columns = ["inserted_timestamp"],
) }}


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
        {% if is_incremental() %}
        AND _inserted_timestamp >= (SELECT max(_inserted_timestamp) FROM {{ this }})
        AND _inserted_timestamp < (SELECT max(_inserted_timestamp) + INTERVAL '1 DAY' FROM {{ this }})
        {% else %}
        AND _inserted_timestamp::date = '2024-05-20'
        {% endif %}
)
select 
    t.block_timestamp,
    t.block_id,
    t.tx_id,
    t.succeeded,
    l.value:index::int as index,
    l.index as log_index,
    l.value:data::string as data,
    l.value:program_id::string as program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','index','log_index']
    ) }} AS transaction_logs_program_data_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from 
    base t
join 
    table(flatten(program_data_logs)) l