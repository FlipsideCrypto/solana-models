{% macro get_token_metadata() %}
{% set query %}
create schema if not exists bronze_api;
create table if not exists bronze_api.token_metadata(address string, data variant, _inserted_timestamp timestamp_ntz);
{% endset %}

{% do run_query(query) %}

{% set query %}
insert into bronze_api.token_metadata(address, data, _inserted_timestamp)
with base as (
    select address
    from {{ ref('seed__missing_token_metadata') }}
    except 
    select address 
    from bronze_api.token_metadata
    limit 25
)
select 
    address,
    livequery_dev.live.udf_api(
      'GET',
      'https://pro-api.solscan.io/v1.0/token/meta?tokenAddress=' || (
        address
      ),
      OBJECT_CONSTRUCT(
        'Accept',
        'application/json',
        'token',
        (
          SELECT
            api_key
          FROM
            crosschain.silver.apis_keys
          WHERE
            api_name = 'solscan'
        )
      ),{}
    ) AS resp,
    sysdate()
from base
;
{% endset %}

{% do run_query(query) %}
{% endmacro %}
