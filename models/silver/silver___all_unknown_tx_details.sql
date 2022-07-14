{{ config(
    materialized = 'view'
) }}

SELECT
    signature as tx_id
FROM
    {{ source(
        'solana_external',
        'stake_account_tx_ids_api'
    ) }}
EXCEPT
SELECT
    tx_id
FROM
    {{ source(
        'solana_external',
        'txs_api'
    ) }}
