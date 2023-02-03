{% macro get_batch_load_logic(model_name) -%}
    {% set query %}
    select max(_inserted_timestamp)::date::string from {{ model_name }};
    {% endset %}

    {% set max_date = run_query(query).columns[0].values()[0] %}

    {% if max_date >= '2022-09-01' and max_date < '2022-09-05' %}
        {{ "and _inserted_timestamp between (select max(_inserted_timestamp) from " ~ model_name ~ ") and (select dateadd('hour',4,max(_inserted_timestamp)) from " ~ model_name ~ ")" }}
    {% elif max_date >= '2022-09-05' and max_date < '2023-01-31' %}
        {{ "and _inserted_timestamp between (select max(_inserted_timestamp) from " ~ model_name ~ ") and (select dateadd('day',15,max(_inserted_timestamp)) from " ~ model_name ~ ")" }}
    {% else %}
        {{ "and _inserted_timestamp >= (select max(_inserted_timestamp) from " ~ model_name}}
    {% endif %}
{%- endmacro %}