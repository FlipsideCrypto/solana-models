-- depends_on: {{ ref('silver__nft_sales_magic_eden_cnft') }}
{{ config(
    materialized = 'incremental',
    tags = ['bronze_api'],
    cluster_by = ['end_inserted_timestamp::date']
) }}

-- new
{% if execute %}
    {% set query %}
        CREATE OR REPLACE TEMPORARY TABLE bronze_api.parse_compressed_nft_sales_magic_eden__intermediate_tmp AS 
        SELECT
            block_timestamp,
            block_id,
            tx_id,
            _inserted_timestamp
        FROM
            {{ ref('silver__nft_sales_magic_eden_cnft') }}
        {% if is_incremental() %}
        WHERE
            _inserted_timestamp >= (
                SELECT
                    MAX(end_inserted_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
        qualify(ROW_NUMBER() over (ORDER BY _inserted_timestamp)) <= 9000
    {% endset %}

    {% do run_query(query) %}
    {% set min_max = run_query("""SELECT min(_inserted_timestamp), max(_inserted_timestamp) FROM bronze_api.parse_compressed_nft_sales_magic_eden__intermediate_tmp""").columns %}
    {% set min_inserted_timestamp = min_max[0][0] %}
    {% set max_inserted_timestamp = min_max[1][0] %}
{% endif %}

WITH collection_subset AS (
    SELECT
        *
    FROM
        bronze_api.parse_compressed_nft_sales_magic_eden__intermediate_tmp
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
        JOIN TABLE(FLATTEN(e.inner_instruction :instructions)) ii
    WHERE
        e.program_id = 'M3mxk5W2tt27WGT7THox7PmgRDp4m6NEhL5xvxrBfS1'
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