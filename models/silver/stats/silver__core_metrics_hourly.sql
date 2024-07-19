{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['curated','xscheduled_non_core']
) }}
/* run incremental query to get relevant dates */
{% if execute %}

{% if is_incremental() %}
{% set query_1 = """ CREATE OR REPLACE TEMPORARY TABLE silver.core_metrics_hourly__intermediate_tmp AS SELECT distinct(block_timestamp)::date as dist_block_ts FROM """ ~ ref('silver__transactions') ~ """ WHERE _inserted_timestamp >= (SELECT MAX(_INSERTED_TIMESTAMP) FROM """ ~ this ~ """)""" %}
{% do run_query(
    query_1
) %}
{% endif %}
{% endif %}

WITH block_stats AS (

    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        MIN(block_id) AS block_number_min,
        MAX(block_id) AS block_number_max,
        COUNT(
            DISTINCT block_id
        ) AS block_count,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_timestamp_hour < DATE_TRUNC(
            'hour',
            CURRENT_TIMESTAMP
        )

{% if is_incremental() %}
AND block_timestamp :: DATE IN (
    SELECT
        dist_block_ts
    FROM
        silver.core_metrics_hourly__intermediate_tmp
)
{% else %}
    AND block_id > 39824213
{% endif %}
GROUP BY
    1
),
tx_stats AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp_hour,
        COUNT(
            DISTINCT tx_id
        ) AS transaction_count,
        COUNT(
            DISTINCT CASE
                WHEN succeeded THEN tx_id
            END
        ) AS transaction_count_success,
        COUNT(
            DISTINCT CASE
                WHEN NOT succeeded THEN tx_id
            END
        ) AS transaction_count_failed,
        COUNT(
            DISTINCT signers [0]
        ) AS unique_signers_count,
        SUM(fee) AS total_fees,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp_hour < DATE_TRUNC(
            'hour',
            CURRENT_TIMESTAMP
        )

{% if is_incremental() %}
AND block_timestamp :: DATE IN (
    SELECT
        dist_block_ts
    FROM
        silver.core_metrics_hourly__intermediate_tmp
)
{% else %}
    AND block_id > 39824213
{% endif %}
GROUP BY
    1
)
SELECT
    A.block_timestamp_hour,
    A.block_number_min,
    A.block_number_max,
    A.block_count,
    b.transaction_count,
    b.transaction_count_success,
    b.transaction_count_failed,
    b.unique_signers_count,
    b.total_fees,
    GREATEST(
        A._inserted_timestamp,
        b._inserted_timestamp
    ) AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['a.block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    block_stats A
    LEFT JOIN tx_stats b
    ON A.block_timestamp_hour = b.block_timestamp_hour
