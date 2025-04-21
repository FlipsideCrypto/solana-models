{{
    config(
        materialized = 'incremental',
        cluster_by = '_partition_id',
        post_hook = ['DELETE FROM {{ this }} WHERE _partition_id < (SELECT max(_partition_id)-1680 FROM {{ this }})'],
        full_refresh = false,
    )
}}

{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT 
                MAX(_partition_id) AS max_partition_id,
                MAX(_inserted_timestamp) AS max_inserted_timestamp
            FROM {{ this }}
        {% endset %}

        {% set result = run_query(query) %}
        {% set max_partition_id = result.columns[0].values()[0] %}
        {% set max_inserted_timestamp = result.columns[1].values()[0] %}
    {% endif %}
{% endif %}

SELECT
    to_timestamp_ntz(t.value:"result.blockTime"::int) AS block_timestamp,
    t.block_id,
    t.value:array_index::int AS tx_index,
    t.data,
    t._partition_id,
    t._inserted_timestamp
FROM
    {{ ref('bronze__streamline_block_txs_2') }} AS t
WHERE
    {% if is_incremental() %}
    _partition_id >= '{{ max_partition_id }}'
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% else %}
    _partition_id = (SELECT max(_partition_id) FROM {{ source('solana_streamline', 'complete_block_txs_2') }}) -- Reference this once to get a starting point
    {% endif %}
