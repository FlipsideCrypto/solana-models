{{ config(
    materialized = 'incremental',
    unique_key = ['block_id'],
    enabled = false
) }}

SELECT
    VALUE :id AS block_id,
    CASE
        WHEN VALUE :error :code :: NUMBER = -32009 THEN 0
        ELSE ARRAY_SIZE(
            VALUE :result :signatures :: ARRAY
        )
    END AS tx_count,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__blocks_tx_count') }},
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
