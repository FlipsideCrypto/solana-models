{% macro get_batch_load_logic(model_name, batch_size_days, end_date) -%}
    {% set query %}
    select max(_inserted_timestamp)::date::string from {{ model_name }};
    {% endset %}

    {% set max_date = run_query(query).columns[0].values()[0] %}
    {% if max_date < '2022-08-24' %}
        {{ "and _inserted_timestamp between (select max(_inserted_timestamp) from " ~ model_name ~ ") and (select dateadd('hour',12,max(_inserted_timestamp)) from " ~ model_name ~ ")" }}
    {% elif max_date >= '2022-08-24' and max_date < '2022-08-31' %}
        {{ "and _inserted_timestamp between (select max(_inserted_timestamp) from " ~ model_name ~ ") and '2022-08-31 01:00:00.000' " }}
    {% elif max_date == '2022-09-01' %}
        {{ "and _inserted_timestamp between (select max(_inserted_timestamp) from " ~ model_name ~ ") and (select dateadd('hour',8,max(_inserted_timestamp)) from " ~ model_name ~ ")" }}
    {% elif max_date >= '2022-08-31' and max_date < '2022-09-05' %}
        {{ "and _inserted_timestamp between (select max(_inserted_timestamp) from " ~ model_name ~ ") and (select dateadd('hour',4,max(_inserted_timestamp)) from " ~ model_name ~ ")" }}
    {% elif max_date >= '2022-09-05' and max_date < end_date %}
        {{ "and _inserted_timestamp between (select max(_inserted_timestamp) from " ~ model_name ~ ") and (select dateadd('day',"~ batch_size_days ~",max(_inserted_timestamp)) from " ~ model_name ~ ")" }}
    {% else %}
        {{ "and _inserted_timestamp >= (select max(_inserted_timestamp) from " ~ model_name ~ ")"}}
    {% endif %}
{%- endmacro %}

{% macro get_batch_load_logic_with_alias(model_name, batch_size_days, end_date, alias) -%}
    {% set query %}
    select max(_inserted_timestamp)::date::string from {{ model_name }};
    {% endset %}

    {% set max_date = run_query(query).columns[0].values()[0] %}

    {% if max_date >= '2022-09-01' and max_date < '2022-09-05' %}
        {{ "and " ~ alias ~ "._inserted_timestamp between (select max(_inserted_timestamp) from " ~ model_name ~ ") and (select dateadd('hour',4,max(_inserted_timestamp)) from " ~ model_name ~ ")" }}
    {% elif max_date >= '2022-09-05' and max_date < end_date %}
        {{ "and " ~ alias ~ "._inserted_timestamp between (select max(_inserted_timestamp) from " ~ model_name ~ ") and (select dateadd('day',"~ batch_size_days ~",max(_inserted_timestamp)) from " ~ model_name ~ ")" }}
    {% else %}
        {{ "and " ~ alias ~ "._inserted_timestamp >= (select max(_inserted_timestamp) from " ~ model_name ~ ")"}}
    {% endif %}
{%- endmacro %}

{% macro dispatch_github_workflow(workflow_name) %}
    {% set context_query %}
        SET LIVEQUERY_CONTEXT = '{"userId":"SYSTEM"}';
    {% endset %}
    {% do run_query(context_query) %}
    {% set query %}
        SELECT github_actions.workflow_dispatches('FlipsideCrypto', 'solana-models', '{{ workflow_name }}.yml', NULL)
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}