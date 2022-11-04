{{ config(
    materialized = 'incremental',
    unique_key = "signer",
    incremental_strategy = 'delete+insert',
    cluster_by = 'signer'
) }}

WITH base_min_signers AS (

    SELECT
        signer,
        MIN(b_date) AS b_date
    FROM
        {{ ref('silver__daily_signers') }}
    GROUP BY
        signer
),
base_max_signers AS (
    SELECT
        signer,
        MAX(b_date) AS b_date
    FROM
        {{ ref('silver__daily_signers') }}
    GROUP BY
        signer
),
first_last_programs AS (
    SELECT
        signer,
        FIRST_VALUE(
            first_program_id ignore nulls
        ) over (
            PARTITION BY signer
            ORDER BY
                b_date
        ) AS first_program_id,
        FIRST_VALUE(
            last_program_id ignore nulls
        ) over (
            PARTITION BY signer
            ORDER BY
                b_date DESC
        ) AS last_program_id
    FROM
        {{ ref('silver__daily_signers') }}
),
final_signers_agg AS (
    SELECT
        ds.signer,
        flp.first_program_id,
        flp.last_program_id,
        COUNT(*) AS num_days_active,
        SUM(num_txs) AS num_txs,
        array_union_agg(unique_program_ids) AS programs_used,
        SUM(total_fees) AS total_fees,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__daily_signers') }}
        ds
        LEFT OUTER JOIN (
            SELECT
                signer,
                first_program_id,
                last_program_id
            FROM
                first_last_programs
            GROUP BY
                1,
                2,
                3
        ) flp
        ON flp.signer = ds.signer
    GROUP BY
        1,
        2,
        3
),
final_min_signers AS (
    SELECT
        ms.signer,
        ms.b_date AS first_tx_date
    FROM
        base_min_signers ms
        INNER JOIN {{ ref('silver__daily_signers') }}
        sd
        ON sd.signer = ms.signer
        AND sd.b_date = ms.b_date
),
final_max_signers AS (
    SELECT
        ms.signer,
        ms.b_date AS last_tx_date
    FROM
        base_max_signers ms
        INNER JOIN {{ ref('silver__daily_signers') }}
        sd
        ON sd.signer = ms.signer
        AND sd.b_date = ms.b_date
)
SELECT
    s_min.*,
    s_agg.first_program_id,
    s_max.last_tx_date,
    s_agg.last_program_id,
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
