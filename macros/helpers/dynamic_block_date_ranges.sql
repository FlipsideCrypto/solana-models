{% macro dynamic_block_range_predicate(source, predicate_column) -%}
    {% set predicate_column_data_type_query %}
        SELECT typeof({{predicate_column}}::variant)
        FROM {{ source }}
        WHERE {{predicate_column}} IS NOT NULL
        LIMIT 1;
    {% endset %}
    {% set predicate_column_data_type = run_query(predicate_column_data_type_query).columns[0].values()[0] %}

    {% set get_limits_query %}
        WITH full_range AS (
            SELECT
                MIN(
                    {{ predicate_column }}
                ) AS full_range_start,
                MAX(
                    {{ predicate_column }}
                ) AS full_range_end
            FROM
                {{ source }}
        ),
        block_range AS (
            {% if predicate_column_data_type == "INTEGER" %}
                {{ dbt_utils.generate_series(
                    start = (select full_range_start from full_range),
                    end = (select full_range_end from full_range),
                    step = '1'
                ) }}
            {% else %}
                {{ dbt_utils.generate_series(
                    start_date = (select full_range_start from full_range),
                    end_date = (select full_range_end from full_range),
                    step = '1 day'
                ) }}
            {% endif %}
            -- SELECT
            --     date_day,
            --     ROW_NUMBER() over (
            --         ORDER BY
            --             date_day
            --     ) - 1 AS rn
            -- FROM
            --     crosschain.core.dim_dates
            -- WHERE
            --     date_day BETWEEN (
            --         SELECT
            --             full_range_start
            --         FROM
            --             full_range
            --     )
            --     AND (
            --         SELECT
            --             full_range_end
            --         FROM
            --             full_range
            --     )
        ),
        partition_block_counts AS (
            SELECT
                b.date_day AS predicate_value,
                COUNT(r.{{ predicate_column }}) AS count_in_window
            FROM
                block_range b
                LEFT OUTER JOIN {{ source }}
                r
                ON r.{{ predicate_column }} = b.date_day
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
        /* need to have something or it will error since 'dynamic_block_date_ranges' is not a column */
    {% endif %}

    {{ return(predicate_override) }}
{% endmacro %}
