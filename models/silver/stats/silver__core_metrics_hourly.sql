{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['curated','scheduled_non_core']
) }}
/* run incremental query to get relevant dates */
{% if execute %}

{% if is_incremental() %}
{% set query_its %}

SELECT
    MAX(_inserted_timestamp) _inserted_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_its = run_query(query_its).columns [0].values() [0] %}
    {% set query %}
SELECT
    COALESCE(
        NULLIF(
            LISTAGG(
                DISTINCT block_timestamp :: DATE,
                ''','''
            ),
            ''
        ),
        SYSDATE() :: DATE
    ) block_dates
FROM
    {{ ref('silver__transactions') }}
WHERE
    _inserted_timestamp >= '{{ max_its }}' {% endset %}
    {% set block_dates = run_query(query).columns [0].values() [0] %}
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
AND block_timestamp :: DATE IN(
    '{{ block_dates }}'
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
            1
        ) AS transaction_count,
        SUM(
            CASE
                WHEN succeeded THEN 1
                ELSE 0
            END
        ) AS transaction_count_success,
        SUM(
            CASE
                WHEN NOT succeeded THEN 1
                ELSE 0
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
AND block_timestamp :: DATE IN(
    '{{ block_dates }}'
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
