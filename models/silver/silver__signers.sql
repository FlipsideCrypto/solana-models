{{ config(
  materialized = 'incremental',
  unique_key = "signer",
  incremental_strategy = 'delete+insert',
  cluster_by = 'signer'
) }}

WITH base_min_signers AS (
    SELECT
        signer, 
        min(b_date) AS b_date
    FROM 
        {{ ref('silver__daily_signers') }}
    GROUP BY 
        signer
),
base_max_signers AS (
    SELECT 
        signer, 
        max(b_date) as b_date
    FROM 
        {{ ref('silver__daily_signers') }}
    GROUP BY 
        signer
),
final_signers_agg AS (
    select
        signer, 
        count(*) AS num_days_active, 
        sum(num_txs) AS num_txs,
        array_union_agg(unique_program_ids) AS programs_used,
        sum(total_fees) AS total_fees, 
        max(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__daily_signers') }}
    GROUP BY 
        signer
),
final_min_signers AS (
    SELECT
        ms.signer, 
        ms.b_date AS first_tx_date,
        sd.first_program_id
    FROM 
        base_min_signers ms
    INNER JOIN {{ ref('silver__daily_signers') }} sd 
    ON sd.signer = ms.signer
    AND sd.b_date = ms.b_date
),
final_max_signers AS (
    SELECT 
        ms.signer, 
        ms.b_date AS last_tx_date,
        sd.last_program_id
    FROM base_max_signers ms
    
    INNER JOIN {{ ref('silver__daily_signers') }} sd
    ON sd.signer = ms.signer 
    AND sd.b_date = ms.b_date
)
SELECT
    s_min.*,
    s_max.last_tx_date,
    s_max.last_program_id,
    s_agg.num_days_active, 
    s_agg.num_txs,
    s_agg.total_fees,
    s_agg.programs_used, 
    s_agg._inserted_timestamp
FROM 
    final_min_signers s_min
JOIN final_max_signers s_max
ON s_max.signer = s_min.signer

JOIN final_signers_agg s_agg
ON s_agg.signer = s_min.signer