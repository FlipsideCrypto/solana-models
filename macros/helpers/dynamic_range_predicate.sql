{% macro dynamic_range_predicate(source, predicate_column, output_alias="") -%}
    {% set supported_data_types = ["INTEGER","DATE"] %}
    {% set predicate_column_data_type_query %}
        SELECT typeof({{predicate_column}}::variant)
        FROM {{ source }}
        WHERE {{predicate_column}} IS NOT NULL
        LIMIT 1;
    {% endset %}
    {% set predicate_column_data_type = run_query(predicate_column_data_type_query).columns[0].values()[0] %}
    

    {% if predicate_column_data_type not in supported_data_types %}
        {{ exceptions.raise_compiler_error("Data type of "~ predicate_column_data_type ~" is not supported, use one of "~ supported_data_types ~" column instead") }}
    {% endif %}

    {% set get_start_end_query %}
        SELECT
            MIN(
                {{ predicate_column }}
            ) AS full_range_start,
            MAX(
                {{ predicate_column }}
            ) AS full_range_end
        FROM
            {{ source }}
    {% endset %}
    {% set start_end_results = run_query(get_start_end_query).columns %}
    {% set start_preciate_value = start_end_results[0].values()[0] %}
    {% set end_predicate_value = start_end_results[1].values()[0] %}

    {% set get_limits_query %}
        WITH block_range AS (
            {% if predicate_column_data_type == "INTEGER" %}
                SELECT 
                    SEQ4() + {{ start_preciate_value }} as predicate_value
                FROM 
                    TABLE(GENERATOR(rowcount => {{ end_predicate_value - start_preciate_value }}+1))
            {% else %}
                SELECT
                    date_day as predicate_value
                FROM
                    crosschain.core.dim_dates
                WHERE
                    date_day BETWEEN '{{ start_preciate_value }}' AND '{{ end_predicate_value }}'
            {% endif %}
        ),
        partition_block_counts AS (
            SELECT
                b.predicate_value,
                COUNT(r.{{ predicate_column }}) AS count_in_window
            FROM
                block_range b
                LEFT OUTER JOIN {{ source }}
                r
                ON r.{{ predicate_column }} = b.predicate_value
            GROUP BY
                1
        ),
        range_groupings AS (
            SELECT
                predicate_value,
                count_in_window,
                conditional_change_event(
                    count_in_window > 0
                ) over (
                    ORDER BY
                        predicate_value
                ) AS group_val
            FROM
                partition_block_counts
        ),
        contiguous_ranges AS (
            SELECT
                MIN(predicate_value) AS start_value,
                MAX(predicate_value) AS end_value
            FROM
                range_groupings
            WHERE
                count_in_window > 0
            GROUP BY
                group_val
        ),
        between_stmts AS (
            SELECT
                CONCAT(
                    '{{ output_alias~"." if output_alias else "" }}',
                    '{{ predicate_column }} between \'',
                    start_value,
                    '\' and \'',
                    end_value,
                    '\''
                ) AS b
            FROM
                contiguous_ranges
        )
        SELECT
            CONCAT('(', LISTAGG(b, ' OR '), ')')
        FROM
            between_stmts 
    {% endset %}
    
    {% set between_stmts = run_query(get_limits_query).columns[0].values()[0] %}

    {% if between_stmts != '()' %}
        /* in case empty update array */
        {% set predicate_override = between_stmts %}
    {% else %}
        {% set predicate_override = '1=1' %}
        /* need to have something or it will error since it expects at least 1 predicate */
    {% endif %}

    {{ return(predicate_override) }}
{% endmacro %}
