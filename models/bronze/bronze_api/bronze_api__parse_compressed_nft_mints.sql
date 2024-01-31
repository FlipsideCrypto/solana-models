-- depends_on: {{ ref('silver__nft_compressed_mints_onchain') }}
-- depends_on: {{ ref('silver__events') }}
{{ config(
    materialized = 'incremental',
    tags = ['bronze_api'],
    cluster_by = ['end_inserted_timestamp::date']
) }}

{% if execute %}
    {% set query = """
        CREATE OR REPLACE TEMPORARY TABLE bronze_api.parse_compressed_nft_mints__intermediate_tmp AS 
        SELECT
            block_timestamp,
            block_id,
            tx_id,
            _inserted_timestamp
        FROM
            """ ~ ref('silver__nft_compressed_mints_onchain')
    %}
    {% set incr = "" %}
    {% if is_incremental() %}
        {% set incr = """
            WHERE
                _inserted_timestamp >= (
                    SELECT
                        MAX(end_inserted_timestamp)
                    FROM
                        """ ~ this ~ """
                )
        """ %}
    {% endif %}
    {% set query2 = """
        qualify(ROW_NUMBER() over (
        ORDER BY
            _inserted_timestamp)) <= 2000""" %}
    {% do run_query(query ~ incr ~ query2) %}
    {% set min_inserted_timestamp = run_query("""SELECT min(_inserted_timestamp) FROM bronze_api.parse_compressed_nft_mints__intermediate_tmp""").columns[0].values()[0] %}
    {% set max_inserted_timestamp = run_query("""SELECT max(_inserted_timestamp) FROM bronze_api.parse_compressed_nft_mints__intermediate_tmp""").columns[0].values()[0] %}

    {% set call_api_query %}
        CREATE OR REPLACE TEMPORARY TABLE bronze_api.parse_compressed_nft_mints__final_tmp AS 
        WITH collection_subset AS (
            SELECT
                distinct tx_id, block_timestamp
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
                    rn / 200
                ) AS gn,
                e._inserted_timestamp AS event_inserted_timestamp
            FROM
                collection_subset C
                JOIN {{ ref('silver__events') }}
                e
                ON C.tx_id = e.tx_id
                JOIN TABLE(FLATTEN(e.inner_instruction :instructions)) ii
            WHERE
                e.program_id IN (
                    'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY',
                    '1atrmQs3eq1N2FEYWu6tyTXbCjP4uQwExpjtnhXtS8h'
                )
                AND e.block_timestamp :: DATE = C.block_timestamp :: DATE
                AND ii_program_id = 'noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV'
                AND e._inserted_timestamp between '{{ min_inserted_timestamp }}' and '{{ max_inserted_timestamp }}'
                AND NOT startswith(DATA, '2GJh')
        )
        SELECT
            ARRAY_AGG(request) AS batch_request,
            streamline.udf_decode_compressed_mint_change_logs(batch_request) AS responses,
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
    {% endset %}

    {% do run_query(call_api_query) %}
{% endif %}

SELECT
    *
FROM 
    bronze_api.parse_compressed_nft_mints__final_tmp

