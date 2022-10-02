{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', signer, b_date)",
  incremental_strategy = 'delete+insert',
  cluster_by = 'signer'
) }}

WITH 
{% if is_incremental() %}
dates_changed AS (
    SELECT
        DISTINCT block_timestamp :: date AS block_timestamp_date
    FROM
        {{ ref('silver__transactions2') }}
    WHERE _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM 
            {{ this }}
    )
),
{% endif %}

b AS (
    SELECT 
        s.value::string AS signer, 
        block_timestamp::date AS b_date,
        first_value(tx_id) over (partition by signer, b_date order by block_timestamp) AS first_tx,
        last_value(tx_id) over (partition by signer, b_date order by block_timestamp) AS last_tx,
        *
    FROM 
        {{ ref('silver__transactions2') }} t,
    table(flatten(signers)) s
    WHERE 
        t.block_timestamp::date between '2022-08-01' AND '2022-08-10'
    {% if is_incremental() and env_var(
        'DBT_IS_BATCH_LOAD',
        "false"
    ) == "true" %}
    AND
        block_timestamp BETWEEN (
            SELECT
                LEAST(COALESCE(MAX(block_timestamp), CURRENT_DATE-10)
            FROM
                {{ this }}
            )
            AND (
            SELECT
                LEAST(COALESCE(MAX(block_timestamp), CURRENT_DATE-10)
            FROM
                {{ this }}
            ) 
    {% elif is_incremental() %}
        AND b_date IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        )
    {% endif %}
),
c AS (
    SELECT
        tx_id, 
        program_id, 
        index,
        _inserted_timestamp
    FROM 
        {{ ref('silver__events2') }} e
    WHERE 
        e.block_timestamp::date between '2022-08-01' AND '2022-08-10'
    {% if is_incremental() and env_var(
        'DBT_IS_BATCH_LOAD',
        "false"
    ) == "true" %}
    AND
        e.block_timestamp BETWEEN (
            SELECT
                LEAST(COALESCE(MAX(block_timestamp), CURRENT_DATE-10)
            FROM
                {{ this }}
            )
            AND (
            SELECT
                LEAST(COALESCE(MAX(block_timestamp), CURRENT_DATE-10)
            FROM
                {{ this }}
            ) 
    {% elif is_incremental() %}
        AND e.block_timestamp::date IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        )
    {% endif %}
),
base_programs AS (
    SELECT
        tx_id, 
        array_agg(program_id) within group (order by index) AS program_ids,
        program_ids[0]::string AS first_program_id,
        program_ids[array_size(program_ids)-1]::string as last_program_id
    FROM 
        c
    GROUP BY 
        tx_id
),
first_last_programs AS (
    SELECT
        b.signer,
        b.b_date,
        b.tx_id,
        first_value(first_program_id) over (partition by signer, b_date order by block_timestamp) AS first_program_id,
        last_value(last_program_id) over (partition by signer, b_date order by block_timestamp) AS last_program_id, 
        last_value(b._inserted_timestamp) over (partition by signer, b_date order by block_timestamp) AS _inserted_timestamp
    FROM 
        b
    LEFT OUTER JOIN base_programs p 
    ON p.tx_id = b.tx_id
),
final_programs AS (
    SELECT 
        b.signer, 
        b.b_date,
        b.first_program_id, 
        b.last_program_id, 
        array_union_agg(p.program_ids) as unique_program_ids, 
        b._inserted_timestamp
    FROM 
        first_last_programs b
    LEFT OUTER JOIN base_programs p 
    ON p.tx_id = b.tx_id
    GROUP BY 
        b.signer, 
        b.b_date, 
        b.first_program_id, 
        b.last_program_id, 
        b._inserted_timestamp
),
final_fees AS (
    SELECT 
        signer,
        b_date,
        sum(fee) AS total_fees
    FROM 
        b
    WHERE 
        index = 0
    GROUP BY 
        signer, 
        b_date
),
final_num_txs AS (
    SELECT 
        signer,
        b_date,
        first_tx,
        last_tx,
        count(*) AS num_txs
    FROM 
        b
    GROUP BY 
        signer, 
        b_date, 
        first_tx, 
        last_tx
)
SELECT 
    s.*, 
    f.total_fees, 
    p.first_program_id, 
    p.last_program_id, 
    p.unique_program_ids, 
    p._inserted_timestamp
FROM 
    final_num_txs s
LEFT OUTER JOIN final_fees f 
ON f.signer = s.signer
AND f.b_date = s.b_date

LEFT OUTER JOIN final_programs p 
ON p.signer = s.signer
AND p.b_date = s.b_date