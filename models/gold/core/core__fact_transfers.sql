{{ config(
    materialized = 'view',
    tags = ['scheduled_core']
) }}

SELECT 
    block_timestamp,
    block_id,
    tx_id,
    index,
    tx_from,
    tx_to,
    amount,
    mint
FROM
    {{ ref('silver__transfers') }}
WHERE 
    succeeded
