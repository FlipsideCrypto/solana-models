{{ config(
    materialized = 'view'
) }}

WITH base AS (

    SELECT
        DISTINCT stake_account
    FROM
        {{ ref('core__ez_staking_lp_actions') }}
    WHERE
        vote_account IS NULL
    EXCEPT
    SELECT
        DISTINCT stake_account
    FROM
        {{ ref('core__ez_staking_lp_actions') }}
    WHERE
        vote_account IS NOT NULL
),
validator_keys as (
    SELECT
        VALUE :nodePubkey :: STRING AS node_pubkey
    FROM
        {{ source(
            'solana_external',
            'validator_metadata_api'
        ) }}
)
SELECT
    stake_account
FROM
    base
WHERE 
    stake_account <> 'FeD1HoB2dyEZnxYpzUnkmQm9jTTY3D7cTf9TFaMGeBii' -- this thing has a ton of txs...it does voting doesnt seem like a stake account
AND
    stake_account not in (select node_pubkey from validator_keys)
EXCEPT
SELECT
    account
FROM
    {{ source(
        'solana_external',
        'stake_account_tx_ids_api'
    ) }}
