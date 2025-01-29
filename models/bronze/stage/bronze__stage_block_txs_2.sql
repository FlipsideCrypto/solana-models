{{
    config(
        materialized = 'incremental',
        cluster_by = '_partition_id',
        post_hook = ['DELETE FROM {{ this }} WHERE _partition_id < (SELECT max(_partition_id)-20 FROM {{ this }})'],
        full_refresh = false,
    )
}}

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
    _partition_id >= (SELECT max(_partition_id) FROM {{ this }})
    AND _inserted_timestamp >= (SELECT max(_inserted_timestamp) FROM {{ this }})
    {% else %}
    _partition_id = (SELECT max(_partition_id) FROM {{ source('solana_streamline', 'complete_block_txs_2') }}) -- Reference this once to get a starting point
    {% endif %}
