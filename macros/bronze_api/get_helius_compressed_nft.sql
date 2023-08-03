{% macro get_helius_compressed_nft() %}
  {% set final_calls_query %}
  CREATE temporary TABLE final_calls AS
SELECT
  ARRAY_AGG(
    { 'id': 'my-id',
    'jsonrpc': '2.0',
    'method': 'searchAssets',
    'params':
      { 'compressed': TRUE,
      'grouping': [ 'collection', collection_mint],
      'page': page,
      'limit': LIMIT, 
      'sortBy':{ 'sortBy': 'created', 'sortDirection': 'desc' }} 
    }
  ) calls, 
  collection_mint AS nft_collection_mint,
  page,
  LIMIT, 
  -- FLOOR((ROW_NUMBER() OVER(ORDER BY collection_mint, page) - 1) / 2) as batch_id
  (ROW_NUMBER() over(ORDER BY collection_mint, page) - 1) AS batch_id
FROM
  (
    WITH counted_items AS (
      SELECT
        collection_mint,
        COUNT(*) AS item_count,
        CEIL((COUNT(*)) / 1000) AS total_pages
      FROM
        {{ target.database }}.silver.nft_compressed_mints_onchain
      WHERE
        _inserted_timestamp >= (
          SELECT
            MAX(_inserted_timestamp)
          FROM
            {{ target.database }}.silver.nft_compressed_mints
        )
      GROUP BY
        collection_mint
    ),
    temp AS (
      SELECT
        collection_mint,
        CASE
          WHEN item_count > 1000
          AND page < total_pages THEN 1000
          WHEN page = total_pages THEN item_count % 1000
          ELSE item_count
        END AS LIMIT, 
        page
      FROM
        counted_items
        CROSS JOIN (
          SELECT
            ROW_NUMBER() over (
              ORDER BY
                SEQ4()
            ) AS page
          FROM
            TABLE(GENERATOR(rowcount => 10000))
        ) page_nums
      WHERE
        page_nums.page <= counted_items.total_pages
        OR (
          item_count > 1000
          AND page_nums.page = counted_items.total_pages
        )
      ORDER BY
        collection_mint,
        page
    )
    SELECT
      *
    FROM
      temp
  )
GROUP BY collection_mint, LIMIT, page
ORDER BY batch_id ASC;
{% endset %}
  {% do run_query(final_calls_query) %}
  {% for batch_id in range(0,2500) %}
    {% set results_query %}
  INSERT INTO
    {{ target.database }}.bronze_API.helius_compressed_nfts WITH results AS (
      SELECT
        ethereum.streamline.udf_json_rpc_call(
          'https://rpc.helius.xyz/?api-key=' || (
            SELECT
              api_key
            FROM
              crosschain.silver.apis_keys
            WHERE
              api_name = 'helius'
          ),{},
          calls
        ) AS DATA,
        nft_collection_mint,
        page,
        LIMIT, 
        SYSDATE() AS _inserted_timestamp
      FROM
        final_calls
      WHERE
        batch_id = {{ batch_id }}
    )
  SELECT
    *
  FROM
    results;
{% endset %}
    {% do run_query(results_query) %}
  {% endfor %}
{% endmacro %}
