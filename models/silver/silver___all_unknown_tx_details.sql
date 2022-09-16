{{ config(
    materialized = 'view'
) }}

SELECT
    signature as tx_id
FROM
    {{ source(
        'bronze_streamline',
        'stake_account_tx_ids_api'
    ) }}
EXCEPT
SELECT
    tx_id
FROM
    {{ source(
        'bronze_streamline',
        'txs_api'
    ) }}
