{% macro get_solscan_token_list() %}
  {% set query %}
INSERT INTO
  bronze_API.solscan_TOKEN_list(
    counter,
    DATA,
    _inserted_timestamp
  )
SELECT
  rn,
  ethereum.streamline.udf_api(
    'GET',
    'https://public-api.solscan.io/token/list?sortBy=volume&direction=desc&limit=50&offset=' || (
      rn * 50
    ) :: STRING || '&token=' || (
      SELECT
        api_key
      FROM
        crosschain.silver.apis_keys
      WHERE
        api_name = 'solscan'
    ),{},{}
  ) AS DATA,
  SYSDATE()
FROM
  (
    SELECT
      SEQ4() rn
    FROM
      TABLE(GENERATOR(rowcount => 75))
  );
{% endset %}
  {% do run_query(query) %}
  {% set wait %}
  CALL system$wait(30);
{% endset %}
  {% do run_query(wait) %}
  {% set query %}
INSERT INTO
  bronze_API.solscan_TOKEN_list(
    counter,
    DATA,
    _inserted_timestamp
  )
SELECT
  rn,
  ethereum.streamline.udf_api(
    'GET',
    'https://public-api.solscan.io/token/list?sortBy=volume&direction=desc&limit=50&offset=' || (
      rn * 50
    ) :: STRING || '&token=' || (
      SELECT
        api_key
      FROM
        crosschain.silver.apis_keys
      WHERE
        api_name = 'solscan'
    ),{},{}
  ) AS DATA,
  SYSDATE()
FROM
  (
    SELECT
      SEQ4() rn
    FROM
      TABLE(GENERATOR(rowcount => 150))
    WHERE
      rn > 74
  );
{% endset %}
  {% do run_query(query) %}
{% endmacro %}