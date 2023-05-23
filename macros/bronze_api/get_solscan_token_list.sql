{% macro get_solscan_token_list() %}
  {% set query %}
DECLARE
  counter INTEGER DEFAULT 1;
maximum_count INTEGER DEFAULT 10;
BEGIN
  for i IN 1 TO maximum_count DO
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
      SELECT API_KEY FROM crosschain.silver.apis_keys WHERE API_NAME = 'solscan'
    ),{},{}
  ) AS DATA,
  SYSDATE()
FROM
  (
    SELECT
      SEQ4() rn
    FROM
      TABLE(GENERATOR(rowcount => 262))
    EXCEPT
    SELECT
      counter
    FROM
      bronze_api.solscan_token_list
    LIMIT
      1
  );
call system $ wait(5);
counter:= counter + 1;
END for;
RETURN counter;
END;
{% endset %}
{% do run_query(query) %}
{% endmacro %}