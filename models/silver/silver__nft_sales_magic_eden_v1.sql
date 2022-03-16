{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, mint)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

WITH sales_inner_instructions AS (

  SELECT
    e.block_timestamp,
    e.block_id,
    e.tx_id,
    t.succeeded,
    e.program_id,
    e.index,
    COALESCE(
      i.value :parsed :info :lamports :: NUMBER,
      0
    ) AS amount,
    e.instruction :accounts [0] :: STRING AS purchaser,
    e.instruction :accounts [1] :: STRING AS nft_account,
    e.instruction :accounts [2] :: STRING AS nft_account_2,
    e.ingested_at
  FROM
    {{ ref('silver__events') }} e
    INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
    LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
  WHERE
    program_id = 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8' -- Magic Eden V1 Program ID
    AND ARRAY_SIZE(
      inner_instruction :instructions
    ) > 2

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
AND t.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
post_token_balances AS (
  SELECT
    DISTINCT tx_id,
    account,
    mint
  FROM
    {{ ref('silver___post_token_balances') }}
    p

{% if is_incremental() %}
WHERE
  p.ingested_at :: DATE >= current_date - 2
{% endif %}
)
SELECT
  s.block_timestamp,
  s.block_id,
  s.tx_id,
  s.succeeded,
  s.program_id,
  COALESCE(
    p.mint,
    p2.mint
  ) AS mint,
  s.purchaser,
  SUM(
    s.amount
  ) / pow(
    10,
    9
  ) AS sales_amount,
  s.ingested_at
FROM
  sales_inner_instructions s
  LEFT OUTER JOIN post_token_balances p
  ON p.tx_id = s.tx_id
  AND p.account = s.nft_account
  LEFT OUTER JOIN solana_dev.silver._post_token_balances p2
  ON p2.tx_id = s.tx_id
  AND p2.account = s.nft_account_2
GROUP BY
  s.block_timestamp,
  s.block_id,
  s.tx_id,
  s.succeeded,
  s.program_id,
  COALESCE(
    p.mint,
    p2.mint
  ),
  s.purchaser,
  s.ingested_at
