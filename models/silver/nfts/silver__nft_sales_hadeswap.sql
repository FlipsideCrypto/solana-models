{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, mint)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
  full_refresh = false,
  enabled = false,
) }}

WITH txs AS (

  SELECT
    DISTINCT tx_id,
    program_id
  FROM
    {{ ref('silver__events') }}
  WHERE
    block_timestamp :: DATE >= '2022-09-22'
    AND program_id = 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
buy_txs AS (
  SELECT
    DISTINCT e.tx_id
  FROM
    txs e
    INNER JOIN {{ ref('silver__transactions') }}
    t
    ON e.tx_id = t.tx_id
    LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
  WHERE
    block_timestamp :: DATE >= '2022-09-22'
    AND l.value :: STRING = 'Program log: Instruction: BuyNftFromPair'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
buys AS (
  SELECT
    block_timestamp,
    block_id,
    tt.tx_id,
    succeeded,
    i.value :accounts [6] :: STRING AS mint,
    signers [0] :: STRING AS purchaser,
    i.value :accounts [4] :: STRING AS seller,
    inner_instructions,
    instructions,
    i.index AS instructions_index,
    _inserted_timestamp
  FROM
    {{ ref('silver__transactions') }}
    t
    INNER JOIN buy_txs tt
    ON t.tx_id = tt.tx_id
    LEFT JOIN TABLE(FLATTEN(instructions)) i
  WHERE
    block_timestamp :: DATE >= '2022-09-22'
    AND i.value :programId :: STRING = 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'
    AND i.value :accounts [2] :: STRING = signers [0] :: string

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
buy_amount AS (
  SELECT
    l.tx_id,
    instructions_index,
    SUM(
      i.value :parsed :info :lamports
    ) / POW(
      10,
      9
    ) AS sales_amount
  FROM
    buys l
    INNER JOIN {{ ref('silver__events') }}
    e
    ON l.tx_id = e.tx_id
    AND l.instructions_index = e.inner_instruction :index
    LEFT JOIN TABLE(FLATTEN(e.inner_instruction :instructions)) i
  WHERE
    i.value :parsed :type = 'transfer'
    AND e.block_timestamp :: DATE >= '2022-09-22'

{% if is_incremental() %}
AND e._inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
GROUP BY
  l.tx_id,
  instructions_index
),
lp_txs AS (
  SELECT
    DISTINCT e.tx_id
  FROM
    txs e
    INNER JOIN {{ ref('silver__transactions') }}
    t
    ON e.tx_id = t.tx_id
    LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
  WHERE
    block_timestamp :: DATE >= '2022-09-22'
    AND l.value :: STRING LIKE 'Program log: Instruction: SellNftToLiquidityPair'

{% if is_incremental() %}
AND t._inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
lp_buys AS (
  SELECT
    block_timestamp,
    block_id,
    tt.tx_id,
    succeeded,
    i.value :accounts [4] :: STRING AS mint,
    signers [0] :: STRING AS seller,
    i.value :accounts [5] :: STRING AS purchaser,
    i.index AS instructions_index,
    inner_instructions,
    _inserted_timestamp
  FROM
    {{ ref('silver__transactions') }}
    t
    INNER JOIN lp_txs tt
    ON t.tx_id = tt.tx_id
    LEFT JOIN TABLE(FLATTEN(instructions)) i
  WHERE
    block_timestamp :: DATE >= '2022-09-22'
    AND i.value :accounts [3] :: STRING = signers [0] :: string
    and i.value:programId::string = 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
lp_amount AS (
  SELECT
    l.tx_id,
    instructions_index,
    SUM(
      i.value :parsed :info :lamports
    ) / POW(
      10,
      9
    ) AS sales_amount
  FROM
    lp_buys l
    INNER JOIN {{ ref('silver__events') }} e
    ON l.tx_id = e.tx_id
    AND l.instructions_index = e.inner_instruction :index
    LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
  WHERE
    e.block_timestamp :: DATE >= '2022-09-22'
    AND i.value :parsed :type = 'transfer'

{% if is_incremental() %}
AND e._inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
GROUP BY
  l.tx_id,
  l.instructions_index
)
SELECT
  block_timestamp,
  block_id,
  b.tx_id,
  succeeded,
  'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu' AS program_id,
  mint,
  purchaser,
  seller,
  sales_amount,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
        ['b.tx_id','mint']
  ) }} AS nft_sales_hadeswap_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  buy_amount A
  INNER JOIN buys b
  ON A.tx_id = b.tx_id
  AND A.instructions_index = b.instructions_index
UNION ALL
SELECT
  block_timestamp,
  block_id,
  A.tx_id,
  succeeded,
  'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu' AS program_id,
  mint,
  purchaser,
  seller,
  sales_amount,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
        ['b.tx_id','mint']
  ) }} AS nft_sales_hadeswap_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  lp_amount A
  INNER JOIN lp_buys b
  ON A.tx_id = b.tx_id
  AND A.instructions_index = b.instructions_index
