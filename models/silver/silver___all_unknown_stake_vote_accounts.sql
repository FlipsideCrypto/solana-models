{{ config(
    materialized = 'view'
) }}

SELECT
    DISTINCT stake_account
FROM
    {{ ref('core__fact_ez_staking_lp_actions') }}
WHERE
    vote_account IS NULL
    AND event_type IN (
        'split_source',
        'merge_destination',
        'merge_source'
    )
EXCEPT
SELECT
    account
FROM
    {{ source(
        'solana_external',
        'stake_account_tx_ids_api'
    ) }}
