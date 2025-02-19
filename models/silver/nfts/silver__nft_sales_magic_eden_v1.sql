{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, mint)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  full_refresh = false,
  enabled = false,
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
    e.instruction :accounts [4] :: STRING AS nft_account_3,
    t.signers [0] :: STRING AS signer,
    i.value :parsed :info :newAuthority :: STRING AS new_authority,
    e._inserted_timestamp
  FROM
    {{ ref('silver__events') }}
    e
    INNER JOIN {{ ref('silver__transactions') }}
    t
    ON t.tx_id = e.tx_id
    LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
  WHERE 
    program_id = 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8' -- Magic Eden V1 Program ID
    AND ARRAY_SIZE(
      inner_instruction :instructions
    ) > 2

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
AND
    t.block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2021-09-07')),'2022-10-05')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2021-09-07')),'2022-10-05')
            FROM
                {{ this }}
        )
AND
    e.block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2021-09-07')),'2022-10-05')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2021-09-07')),'2022-10-05')
            FROM
                {{ this }}
        )
{% elif is_incremental() %}
AND e._inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
AND t._inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% else %}
AND 
    e.block_timestamp :: DATE BETWEEN '2021-09-07'
    AND '2021-10-07'
AND 
    t.block_timestamp :: DATE BETWEEN '2021-09-07'
    AND '2021-10-07'

{% endif %}
),
sellers AS (
  SELECT
    tx_id,
    index,
    CASE
      WHEN new_authority <> signer THEN signer
      ELSE nft_account_2
    END AS seller,
    CASE
      WHEN new_authority <> signer THEN nft_account
      ELSE purchaser
    END AS purchaser
  FROM
    sales_inner_instructions
  WHERE
    new_authority IS NOT NULL
),
post_token_balances AS (
  SELECT
    DISTINCT tx_id,
    account,
    mint
  FROM
    {{ ref('silver___post_token_balances') }}
    p

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    p.block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2021-09-07')),'2022-10-05')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2021-09-07')),'2022-10-05')
            FROM
                {{ this }}
        ) 
{% elif is_incremental() %}
WHERE
  p._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
{% else %}
WHERE
    block_timestamp :: DATE BETWEEN '2021-09-07'
    AND '2021-10-07'
{% endif %}
)
SELECT
  s.block_timestamp,
  s.block_id,
  s.tx_id,
  s.index,
  null as inner_index,
  s.succeeded,
  s.program_id,
  COALESCE(
    p.mint,
    p2.mint
  ) AS mint,
  ss.purchaser,
  ss.seller,
  SUM(
    s.amount
  ) / pow(
    10,
    9
  ) AS sales_amount,
  s._inserted_timestamp
FROM
  sales_inner_instructions s
  LEFT OUTER JOIN post_token_balances p
  ON p.tx_id = s.tx_id
  AND p.account = s.nft_account
  LEFT OUTER JOIN post_token_balances p2
  ON p2.tx_id = s.tx_id
  AND p2.account = s.nft_account_2
  LEFT OUTER JOIN sellers ss
  ON ss.tx_id = s.tx_id
  AND ss.index = s.index
GROUP BY
  s.block_timestamp,
  s.block_id,
  s.tx_id,
  s.succeeded,
  s.index,
  s.program_id,
  COALESCE(
    p.mint,
    p2.mint
  ),
  ss.seller,
  ss.purchaser,
  s._inserted_timestamp
