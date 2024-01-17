{{ config(
    materialized = 'view',
    tags = ['scheduled_daily','signers']
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
    COALESCE (
        signers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['signer']
        ) }}
    ) AS ez_signers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__signers') }}
