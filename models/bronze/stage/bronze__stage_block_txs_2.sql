{{
    config(
        materialized = 'incremental',
        cluster_by = '_partition_id',
        post_hook = ['DELETE FROM {{ this }} WHERE _partition_id < (SELECT max(_partition_id)-20 FROM {{ this }})'],
        full_refresh = false,
    )
}}

SELECT
    *
FROM
    {{ ref('bronze__streamline_block_txs_2') }}
WHERE
    {% if is_incremental() %}
    _partition_id >= (SELECT max(_partition_id) FROM {{ this }})
    AND _inserted_timestamp >= (SELECT max(_inserted_timestamp) FROM {{ this }})
    {% else %}
    _partition_id = (SELECT max(_partition_id) FROM {{ source('solana_streamline', 'complete_block_txs_2') }}) -- Reference this once to get a starting point
    {% endif %}
