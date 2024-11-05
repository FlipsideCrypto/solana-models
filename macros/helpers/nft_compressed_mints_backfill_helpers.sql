{% macro nft_compressed_mints_backfill_generate_views() %}
    {% set query %}
        create or replace view bronze_api.nft_compressed_mints_backfill_requests as
        with base_delta as (
            -- 2537463
            select tx_id, block_timestamp
            from silver.nft_compressed_mints_onchain
            where block_timestamp between '2024-02-01' and '2024-03-02'
            except 
            select tx_id, block_timestamp
            from silver.nft_compressed_mints
        )
        select distinct tx_id, block_timestamp
        from base_delta
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}

{% macro nft_compressed_mints_backfill_make_call() %}

    {% set request_batch_setup %}
        create or replace temporary table bronze_api.nft_compressed_mints_backfill_requests_batch as 
        SELECT
            tx_id, 
            block_timestamp
        FROM
            bronze_api.nft_compressed_mints_backfill_requests
        qualify(row_number() over (order by block_timestamp)) <= 1500;
    {% endset %}
    {% do run_query(request_batch_setup) %}

    {% set has_data = run_query("""select (count(*) > 0) as has_data from bronze_api.nft_compressed_mints_backfill_requests_batch;""").columns[0].values()[0] %}

    {% if has_data %}
        {% set min_block_timestamp = run_query("""select min(block_timestamp) from bronze_api.nft_compressed_mints_backfill_requests_batch""").columns[0].values()[0] %}
        {% set max_block_timestamp = run_query("""select max(block_timestamp) from bronze_api.nft_compressed_mints_backfill_requests_batch""").columns[0].values()[0] %}

        {% set make_requests %}
            create or replace temporary table bronze_api.parse_compressed_nft_mints_backfill_responses as
            WITH base AS (
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
                    bronze_api.nft_compressed_mints_backfill_requests_batch C
                    JOIN silver.events
                    e
                    ON e.block_timestamp :: DATE = C.block_timestamp :: DATE
                    AND C.tx_id = e.tx_id
                    JOIN TABLE(FLATTEN(e.inner_instruction :instructions)) ii
                WHERE
                    e.program_id IN (
                        'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY',
                        '1atrmQs3eq1N2FEYWu6tyTXbCjP4uQwExpjtnhXtS8h',
                        'F9SixdqdmEBP5kprp2gZPZNeMmfHJRCTMFjN22dx3akf'
                    )
                    AND ii_program_id = 'noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV'
                    AND e.block_timestamp between '{{ min_block_timestamp }}' and '{{ max_block_timestamp }}'
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
                gn;
        {% endset %}
        {% do run_query(make_requests) %}

        {% set insert_into_bronze_api %}
            insert into bronze_api.parse_compressed_nft_mints
            select *
            from bronze_api.parse_compressed_nft_mints_backfill_responses;
        {% endset %}


        {% set incremental_model %}
            create temporary table silver.nft_compressed_mints__merge_tmp as
            WITH offchain AS (

                SELECT
                    r.value :tx_id :: STRING AS tx_id,
                    r.value :index :: INTEGER AS mint_index,
                    r.value :inner_index :: INTEGER AS mint_inner_index,
                    COALESCE(
                        r.value :mint :: STRING,
                        ''
                    ) AS mint,
                    0.000005 AS mint_price,
                    'So11111111111111111111111111111111111111111' AS mint_currency,
                    'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY' AS program_id,
                    start_inserted_timestamp AS _inserted_timestamp
                FROM
                    bronze_api.PARSE_COMPRESSED_NFT_MINTS_BACKFILL_RESPONSES,
                    TABLE(FLATTEN(responses)) AS r
                WHERE
                    mint <> '' 
                
                qualify(ROW_NUMBER() over (PARTITION BY tx_id, mint
                ORDER BY
                    _inserted_timestamp DESC)) = 1
            ),
            offchain_ordered AS (
                SELECT
                    *,
                    ROW_NUMBER() over (
                        PARTITION BY tx_id
                        ORDER BY
                            mint_index,
                            mint_inner_index
                    ) AS instruction_order
                FROM
                    offchain
            ),
            decoded AS (
                SELECT
                    decoded_instruction :name :: STRING AS instruction_name,
                    *
                FROM
                    silver.decoded_instructions
                WHERE
                    program_id = 'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY'
                
                AND _inserted_timestamp >= (
                    SELECT
                        MIN(start_inserted_timestamp)
                    FROM
                        bronze_api.PARSE_COMPRESSED_NFT_MINTS_BACKFILL_RESPONSES
                )
                AND block_timestamp between '{{ min_block_timestamp }}' and '{{ max_block_timestamp }}'     
            ),
            onchain AS (
                SELECT
                    m.*,
                    d.instruction_name,
                    ROW_NUMBER() over (
                        PARTITION BY m.tx_id
                        ORDER BY
                            m.index,
                            m.inner_index
                    ) AS instruction_order
                FROM
                    silver.nft_compressed_mints_onchain
                    m
                    LEFT OUTER JOIN decoded d
                    ON d.tx_id = m.tx_id
                    AND d.index = m.index
                    AND COALESCE(
                        d.inner_index,
                        -1
                    ) = COALESCE(
                        m.inner_index,
                        -1
                    )
                
                WHERE m._inserted_timestamp >= (
                    SELECT
                        MIN(start_inserted_timestamp)
                    FROM
                        bronze_api.PARSE_COMPRESSED_NFT_MINTS_BACKFILL_RESPONSES
                )
                AND m.block_timestamp between '{{ min_block_timestamp }}' and '{{ max_block_timestamp }}'
                
            )
            SELECT
                A.block_timestamp,
                A.block_id,
                A.succeeded,
                b.tx_id,
                A.leaf_owner,
                A.collection_mint,
                b._inserted_timestamp,
                A.creator_address AS purchaser,
                b.mint,
                b.mint_price,
                b.mint_currency,
                b.program_id,
                A.instruction_name,
                
                
            md5(cast(coalesce(cast(b.tx_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b.mint as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS nft_compressed_mints_id,
                SYSDATE() AS inserted_timestamp,
                SYSDATE() AS modified_timestamp
            FROM
                offchain_ordered b
                LEFT JOIN onchain A
                ON A.tx_id = b.tx_id
                AND A.instruction_order = b.instruction_order
            WHERE
                (
                    A.instruction_name LIKE 'mint%'
                    OR A.instruction_name IS NULL
                );
        {% endset %}

            
        {% set merge_model %}
            MERGE INTO silver.nft_compressed_mints AS target
            USING silver.nft_compressed_mints__merge_tmp AS source
            ON target.tx_id = source.tx_id
            AND target.mint = source.mint
            WHEN MATCHED THEN
                UPDATE SET
                    target.block_timestamp = source.block_timestamp,
                    target.block_id = source.block_id,
                    target.succeeded = source.succeeded,
                    target.leaf_owner = source.leaf_owner,
                    target.collection_mint = source.collection_mint,
                    target._inserted_timestamp = source._inserted_timestamp,
                    target.purchaser = source.purchaser,
                    target.mint_price = source.mint_price,
                    target.mint_currency = source.mint_currency,
                    target.program_id = source.program_id,
                    target.instruction_name = source.instruction_name,
                    target.nft_compressed_mints_id = source.nft_compressed_mints_id,
                    target.modified_timestamp = source.modified_timestamp
            WHEN NOT MATCHED THEN
                INSERT (block_timestamp,
                    block_id,
                    succeeded,
                    tx_id,
                    leaf_owner,
                    collection_mint,
                    _inserted_timestamp,
                    purchaser,
                    mint,
                    mint_price,
                    mint_currency,
                    program_id,
                    instruction_name,
                    nft_compressed_mints_id,
                    inserted_timestamp,
                    modified_timestamp)
                VALUES (source.block_timestamp,
                    source.block_id,
                    source.succeeded,
                    source.tx_id,
                    source.leaf_owner,
                    source.collection_mint,
                    source._inserted_timestamp,
                    source.purchaser,
                    source.mint,
                    source.mint_price,
                    source.mint_currency,
                    source.program_id,
                    source.instruction_name,
                    source.nft_compressed_mints_id,
                    source.inserted_timestamp,
                    source.modified_timestamp);
        {% endset %}
        {% do run_query("BEGIN TRANSACTION; " ~ insert_into_bronze_api ~ " " ~ incremental_model ~ " " ~ merge_model ~ " COMMIT;") %}
    {% endif %}
{% endmacro %}