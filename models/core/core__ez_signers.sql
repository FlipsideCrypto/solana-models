{{ config(
    materialized = 'view'
) }}

SELECT
    signer,
    first_tx_date,
    first_program_id,
    last_tx_date,
    last_program_id,
    num_days_active,
    num_txs,
    total_fees,
    programs_used, 
    nfts_held
FROM
    {{ ref('silver__signers') }}
