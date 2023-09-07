{{ config(
    materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    mint,
    burn_amount,
    burn_authority,
    signers
FROM
    {{ ref('silver__burn_actions') }}
