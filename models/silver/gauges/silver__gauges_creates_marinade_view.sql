{{ config(
  materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signer,
    gauge,
    gaugemeister,
    validator_account
FROM
 {{ source(
    'solana_silver',
    'gauges_creates_marinade'
  ) }}