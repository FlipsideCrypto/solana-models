{% macro if_data_call_function(
        func,
        target
    ) %}
    {% if var(
            "STREAMLINE_INVOKE_STREAMS"
        ) %}
        {% if execute %}
            {{ log(
                "Running macro `if_data_call_function`: Calling udf " ~ func ~ " on " ~ target,
                True
            ) }}
        {% endif %}
    SELECT
        {{ func }}
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                {{ target }}
            LIMIT
                1
        )
    {% else %}
        {% if execute %}
            {{ log(
                "Running macro `if_data_call_function`: NOOP",
                False
            ) }}
        {% endif %}
    SELECT
        NULL
    {% endif %}
{% endmacro %}

{% macro if_data_call_wait() %}
    {% if var(
            "STREAMLINE_INVOKE_STREAMS"
        ) %}
        {% set query %}
    SELECT
        1
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                {{ model.schema ~ "." ~ model.alias }}
            LIMIT
                1
        ) {% endset %}
        {% if execute %}
            {% set results = run_query(
                query
            ) %}
            {% if results %}
                {{ log(
                    "Waiting...",
                    info = True
                ) }}

                {% set wait_query %}
            SELECT
                system$wait(
                    {{ var(
                        "WAIT",
                        600
                    ) }}
                ) {% endset %}
                {% do run_query(wait_query) %}
            {% else %}
            SELECT
                NULL;
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}