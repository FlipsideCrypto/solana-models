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
    ethereum.streamline.udf_api(
        'GET', 
        concat('https://public-api.solscan.io/token/meta?tokenAddress=',
        address,
        '&token=',(
      SELECT API_KEY FROM crosschain.silver.apis_keys WHERE API_NAME = 'solscan')), {}, {}) as resp,
    sysdate()
from base
;
{% endset %}

{% do run_query(query) %}
{% endmacro %}
