{{ config(
    materialized = 'incremental',
    unique_key = ['block_id'],
) }}

SELECT
    VALUE :block_id AS block_id,
    value:result:transactionCount as transaction_count,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__solscan_blocks') }},
    TABLE(FLATTEN(DATA)) d

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
