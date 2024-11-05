{{ config(
    materialized = 'incremental',
    unique_key = ['block_id'],
) }}

SELECT
    block_id,
    data:result:transactionCount as transaction_count,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_solscan_blocks_2') }}
{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            max(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
QUALIFY 
    row_number() over (partition by block_id order by _inserted_timestamp desc, transaction_count desc) = 1