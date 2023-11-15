{{ config(
    materialized = 'incremental',
    tags = ['bronze_api']
) }}

{% if execute %}
    {% set query = """
        SELECT
            min(gn) as min_gn
        FROM
            """ ~ source('bronze_api_prod','parse_compressed_nft_mints_requests') ~ """
        """ 
    %}
    {% set incr = "" %}
    {% if is_incremental() %}
        {% set incr = """
            WHERE event_inserted_timestamp >= (
                SELECT
                    MAX(end_inserted_timestamp)
                FROM
                    """ ~ this ~ """
            )
        """ %}
    {% endif %}
    {% set min_gn = run_query(query ~ incr).columns[0].values()[0] %}
{% endif %}

-- WITH collection_subset AS (

--     SELECT
--         block_timestamp,
--         block_id,
--         tx_id,
--         _inserted_timestamp
--     FROM
--         {{ ref('silver__nft_compressed_mints_onchain') }}

-- {% if is_incremental() %}
-- WHERE
--     _inserted_timestamp >= (
--         SELECT
--             MAX(end_inserted_timestamp)
--         FROM
--             {{ this }}
--     )
-- {% else %}
-- WHERE
--     _inserted_timestamp :: DATE = '2023-02-07'
-- {% endif %}

-- qualify(ROW_NUMBER() over (
-- ORDER BY
--     _inserted_timestamp)) <= 5000
-- ),
-- base AS (
--     SELECT
--         e.tx_id,
--         e.index,
--         ii.index AS inner_index,
--         ii.value :data :: STRING AS DATA,
--         OBJECT_CONSTRUCT(
--             'tx_id',
--             e.tx_id,
--             'index',
--             e.index,
--             'inner_index',
--             inner_index,
--             'instruction_data',
--             DATA
--         ) AS request,
--         ii.value :programId :: STRING AS ii_program_id,
--         ROW_NUMBER() over (
--             ORDER BY
--                 e._inserted_timestamp,
--                 e.tx_id
--         ) AS rn,
--         FLOOR(
--             rn / 1000
--         ) AS gn,
--         e._inserted_timestamp AS event_inserted_timestamp
--     FROM
--         collection_subset C
--         JOIN {{ ref('silver__events') }}
--         e
--         ON C.tx_id = e.tx_id
--         JOIN TABLE(FLATTEN(e.inner_instruction :instructions)) ii
--     WHERE
--         e.program_id IN (
--             'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY',
--             '1atrmQs3eq1N2FEYWu6tyTXbCjP4uQwExpjtnhXtS8h'
--         )
--         AND e.block_timestamp :: DATE = C.block_timestamp :: DATE
--         AND ii_program_id = 'noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV'
-- )
SELECT
    ARRAY_AGG(request) AS batch_request,
    streamline.udf_bulk_parse_compressed_nft_mints(batch_request) AS responses,
    MIN(event_inserted_timestamp) AS start_inserted_timestamp,
    MAX(event_inserted_timestamp) AS end_inserted_timestamp,
    concat_ws(
        '-',
        end_inserted_timestamp,
        gn
    ) AS _id
FROM
    {{ source('bronze_api_prod','parse_compressed_nft_mints_requests') }}
WHERE 
    gn between {{ min_gn }}+1 and {{ min_gn }}+10
GROUP BY
    gn