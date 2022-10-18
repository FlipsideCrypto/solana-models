{% macro run_sp_create_prod_community_clone() %}
{% set clone_query %}
call solana._internal.create_prod_clone('solana', 'solana_community_dev', 'flipside_community_curator');
{% endset %}

{% do run_query(clone_query) %}

{% endmacro %}