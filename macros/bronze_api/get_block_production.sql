{% macro get_block_production() %}
 {% set create_table %}
CREATE TABLE if NOT EXISTS {{ target.database }}.bronze_api.block_production(
    json_data VARIANT,
    epoch varchar,
    calls string,
     _inserted_timestamp timestamp_ntz
  );
{% endset %}
  {% do run_query(create_table) %}
  {% set create_block_prod_call_query %}
  CREATE temporary TABLE bronze_api.block_production_call AS
SELECT
  ARRAY_AGG(
    { 'id': previous_epoch,
    'jsonrpc': '2.0',
    'method': 'getBlockProduction',
    'params': [ { 'commitment': 'confirmed', 'range': { 'firstSlot': previous_epoch_start, 'lastSlot': previous_epoch_end } }] }
  ) calls
FROM
  (
    WITH current_epoch_data AS (
      SELECT
        ethereum.streamline.udf_json_rpc_call(
          'https://api.mainnet-beta.solana.com',{ 'Content-Type': 'application/json' },
          [ { 'id': 'my-id', 'jsonrpc': '2.0', 'method': 'getEpochInfo' } ]
        ) DATA
    ),
    temp AS (
      SELECT
        DATA :data [0] :result :epoch :: INT AS current_epoch,
        current_epoch - 1 AS previous_epoch,
        DATA :data [0] :result :absoluteSlot - DATA :data [0] :result :slotIndex AS current_epoch_start,
        (
          current_epoch_start - 432000
        ) :: INT AS previous_epoch_start,
        (
          current_epoch_start - 1
        ) :: INT AS previous_epoch_end
      FROM
        current_epoch_data
    )
    SELECT
      *
    FROM
      temp
  ) {% endset %}
  {% do run_query(create_block_prod_call_query) %}
  {% set results_query %}
INSERT INTO
    bronze_api.block_production (json_data,epoch,calls,_inserted_timestamp) WITH results AS (
    SELECT
      ethereum.streamline.udf_json_rpc_call(
        'https://api.mainnet-beta.solana.com',{ 'Content-Type': 'application/json' },
        calls
      ),
      calls [0] :id,
      calls :: string,
      TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS _inserted_timestamp
    FROM
      bronze_api.block_production_call
  )
SELECT
  *
FROM
  results;
{% endset %}
  {% do run_query(results_query) %}
{% endmacro %}
