-- depends_on: {{ ref('silver__nft_compressed_mints_onchain') }}
{{ config(
    materialized = 'incremental',
    tags = ['bronze_api'],
    cluster_by = ['end_inserted_timestamp::date']
) }}

{% if execute %}
    {% set query %}
        CREATE OR REPLACE TEMPORARY TABLE bronze_api.parse_compressed_nft_mints__intermediate_tmp AS 
        WITH max_time AS (
            SELECT
                MAX(end_inserted_timestamp) as max_inserted_timestamp
            FROM
                {{ this }}
        ),
        base AS (
            SELECT DISTINCT r.value:tx_id::string AS tx_id
            FROM {{ this }}
            JOIN TABLE(flatten(responses)) r
            WHERE end_inserted_timestamp >= (SELECT max_inserted_timestamp FROM max_time)
        )
        SELECT
            m.block_timestamp,
            m.block_id,
            m.tx_id,
            m.index,
            m._inserted_timestamp
        FROM
            {{ ref('silver__nft_compressed_mints_onchain') }} m
        LEFT OUTER JOIN base b on b.tx_id = m.tx_id
        WHERE 
            b.tx_id IS NULL
        {% if is_incremental() %}
        AND
            _inserted_timestamp >= (SELECT max_inserted_timestamp FROM max_time)
        {% endif %}
        qualify(ROW_NUMBER() over (ORDER BY _inserted_timestamp)) <= 9000
    {% endset %}

    {% do run_query(query) %}
    {% set min_max = run_query("""SELECT min(_inserted_timestamp), max(_inserted_timestamp) FROM bronze_api.parse_compressed_nft_mints__intermediate_tmp""").columns %}
    {% set min_inserted_timestamp = min_max[0][0] %}
    {% set max_inserted_timestamp = min_max[1][0] %}
{% endif %}

WITH collection_subset AS (
    SELECT
        *
    FROM
        bronze_api.parse_compressed_nft_mints__intermediate_tmp
),
base AS (
    SELECT
        e.tx_id,
        e.index,
        ii.index AS inner_index,
        ii.value :data :: STRING AS DATA,
        OBJECT_CONSTRUCT(
            'tx_id',
            e.tx_id,
            'index',
            e.index,
            'inner_index',
            inner_index,
            'instruction_data',
            DATA
        ) AS request,
        ii.value :programId :: STRING AS ii_program_id,
        ROW_NUMBER() over (
            ORDER BY
                e._inserted_timestamp,
                e.tx_id
        ) AS rn,
        FLOOR(
            rn / 150
        ) AS gn,
        e._inserted_timestamp AS event_inserted_timestamp
    FROM
        collection_subset C
        JOIN {{ ref('silver__events') }}
        e
        ON C.tx_id = e.tx_id
        AND C.index = e.index
        JOIN TABLE(FLATTEN(e.inner_instruction :instructions)) ii
    WHERE
        e.program_id IN (
            'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY',
            '1atrmQs3eq1N2FEYWu6tyTXbCjP4uQwExpjtnhXtS8h',
            'F9SixdqdmEBP5kprp2gZPZNeMmfHJRCTMFjN22dx3akf'
        )
        AND e.block_timestamp :: DATE = C.block_timestamp :: DATE
        AND ii_program_id = 'noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV'
        AND e._inserted_timestamp between '{{ min_inserted_timestamp }}' and '{{ max_inserted_timestamp }}'
        AND NOT startswith(DATA, '2GJh')
)
SELECT
    ARRAY_AGG(request) AS batch_request,
    streamline.udf_decode_compressed_mint_change_logs_v2(batch_request) AS responses,
    MIN(event_inserted_timestamp) AS start_inserted_timestamp,
    MAX(event_inserted_timestamp) AS end_inserted_timestamp,
    concat_ws(
        '-',
        end_inserted_timestamp,
        gn
    ) AS _id
FROM
    base
GROUP BY
    gn