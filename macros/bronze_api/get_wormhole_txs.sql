{% macro get_wormhole_txs() %} -- works well just doesnt do anything if no new txs so skips it
 {% set create_table %}
CREATE TABLE if NOT EXISTS {{ target.database }}.bronze_api.wormhole_txs(
    json_data VARIANT,
    page INT,
    tx_count INT,
     _inserted_timestamp timestamp_ntz
  );
{% endset %}
  {% do run_query(create_table) %}

  {% set get_max_page_query %}
SELECT COALESCE(MAX(page), 0) as max_page FROM {{ target.database }}.bronze_api.wormhole_txs
 {% endset %}



 {% set max_page_result = run_query(get_max_page_query) %}
{% if execute %}
    {% set start_page = max_page_result.columns[0].values()[0]|int %}
    
{% endif %}


 {% set max_iterations = 100 %}  -- Adjust based on expected maximum number of pages
  {% set continue_loop = true %}
  {% for i in range(start_page, start_page + max_iterations) %}
    {% if continue_loop %}
      {% set api_query %}
        CREATE OR REPLACE TEMPORARY TABLE api_result AS
        SELECT
          livequery_dev.live.udf_api(
            'GET',
            'https://api.wormholescan.io/api/v1/transactions/?page=' || {{ i }} || '&pageSize=3000&sortOrder=ASC',
            OBJECT_CONSTRUCT('Accept', 'application/json'),{}
          ) AS response_data,
          {{ i }} AS page,
          coalesce(array_size(response_data :data :transactions),0) as tx_count,
          SYSDATE() AS _inserted_timestamp
      {% endset %}
      {% do run_query(api_query) %}

      {% set check_query %}
        SELECT tx_count FROM api_result
      {% endset %}
      {% set check_result = run_query(check_query) %}
      {% set transactions = check_result.columns[0].values()[0] %}

      {% if transactions > 0 %}
        {% set insert_query %}
          INSERT INTO {{ target.database }}.bronze_api.wormhole_txs(json_data, page, tx_count, _inserted_timestamp)
          SELECT response_data, page, tx_count, _inserted_timestamp FROM api_result
        {% endset %}
        {% do run_query(insert_query) %}
      {% else %}
        {% set continue_loop = false %}
      {% endif %}
    {% endif %}   
  {% endfor %}
{% endmacro %}

