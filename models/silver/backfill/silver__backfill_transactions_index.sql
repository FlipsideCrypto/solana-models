-- depends_on: {{ ref('bronze__streamline_block_tx_index_backfill') }}

{{
    config(
        materialized="incremental",
        cluster_by = ['block_timestamp::date','block_id'],
        tags=['tx_index_backfill']
    )
}}

{% if execute %}
    {% if is_incremental() %}
        {% set max_partition_query %}
        SELECT max(_partition_by_created_date) FROM {{ this }}
        {% endset %}
        {% set max_partition = run_query(max_partition_query)[0][0] %}
    {% endif %}
{% endif %}

SELECT 
    block_id,
    to_timestamp_ntz(value:"result.blockTime"::int) AS block_timestamp,
    data::string as tx_id,
    value:array_index::int as tx_index,
    _partition_by_created_date,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id']) }} AS transactions_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if is_incremental() %}
    {{ ref('bronze__streamline_block_tx_index_backfill') }}
    {% else %}
    {{ ref('bronze__streamline_FR_block_tx_index_backfill') }}
    {% endif %}
WHERE 
    data IS NOT NULL
{% if is_incremental() %}
    AND _partition_by_created_date >= {{ max_partition }}
    AND _inserted_timestamp > (SELECT max(_inserted_timestamp) FROM {{ this }}) 
{% endif %}