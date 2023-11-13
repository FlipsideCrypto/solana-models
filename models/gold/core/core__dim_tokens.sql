{{ config(
  materialized = 'view',
  tags = ['scheduled_non_core']
) }}

SELECT
  token_address,
  token_name,
  symbol,
  decimals,
  coin_gecko_id,
  coin_market_cap_id,
  tags,
  logo,
  twitter,
  website,
  description,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__token_metadata') }}
