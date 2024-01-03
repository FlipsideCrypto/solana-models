{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    cluster_by = '_inserted_timestamp::date',
    full_refresh = false
) }}
--update

WITH pre_requests AS (

    SELECT
        *
    FROM
        {{ ref('silver__debridge_events') }}
),
final_requests AS (
    SELECT
        *,(ROW_NUMBER() over (
    ORDER BY
        _inserted_timestamp)) AS batch_id
    FROM
        pre_requests
),
response AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        livequery_dev.live.udf_api(
            'GET',
            'https://stats-api.dln.trade/api/Transaction/' || (
                tx_id
            ) || '/liteModels',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),{}
        ) AS response,
        SYSDATE() AS _inserted_timestamp
    FROM
        final_requests
    WHERE
        batch_id < 50
)
SELECT
    tx_id,
    block_id,
    block_timestamp,
    PARSE_JSON(
        response :data
    ) AS json_data,
    response :status_code AS status_code,
    _inserted_timestamp
FROM
    response
