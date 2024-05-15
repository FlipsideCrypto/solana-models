WITH max_part_id_tmp AS (
  SELECT
    MAX(_partition_id) AS _partition_id
  FROM
    {% if target.database == 'SOLANA' %}
      solana.silver.votes
    {% else %}
      solana_dev.silver.votes
    {% endif %}
  UNION
  SELECT
    MAX(_partition_id)
  FROM
    {% if target.database == 'SOLANA' %}
      solana.silver.transactions
    {% else %}
      solana_dev.silver.transactions
    {% endif %}
),
base AS (
  SELECT
    DISTINCT _partition_id
  FROM
    {% if target.database == 'SOLANA' %}
      solana.streamline.complete_block_txs
    {% else %}
      solana_dev.streamline.complete_block_txs
    {% endif %}
  WHERE
    _partition_id <= (
      SELECT
        MAX(_partition_id)
      FROM
        max_part_id_tmp
    )
),
base_txs AS (
  SELECT
    DISTINCT _partition_id
  FROM
    {{ ref('silver__transactions') }}
  UNION
  SELECT
    DISTINCT _partition_id
  FROM
    {% if target.database == 'SOLANA' %}
      solana.silver.votes
    {% else %}
      solana_dev.silver.votes
    {% endif %}
)
SELECT
  b._partition_id
FROM
  base b
  LEFT OUTER JOIN base_txs t
  ON b._partition_id = t._partition_id
WHERE
  t._partition_id IS NULL
  AND b._partition_id <> 1877 -- seems like this whole partition is skipped slots
  AND b._partition_id > 81886 /* some old partitions never got loaded into silver, 
                                the data has made it into silver through other partitions 
                                and confirmed via other checks.
                                We can start checking for new instances after this partition
                                and consider everything before reconciled. */
