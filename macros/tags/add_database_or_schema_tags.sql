{% macro add_database_or_schema_tags() %}
    {{ set_database_tag_value('blockchain','solana') }}
    {{ set_schema_tag_value('core','schema_type','prod') }}
{% endmacro %}