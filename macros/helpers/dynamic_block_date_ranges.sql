{% macro dynamic_block_date_ranges(source) -%}
    {% set get_limits_query %}
    WITH full_range AS (
        SELECT
            MIN(
                block_timestamp :: DATE
            ) AS full_range_start_block_date,
            MAX(
                block_timestamp :: DATE
            ) AS full_range_end_block_date
        FROM
            {{ source }}
    ),
    block_range AS (
        SELECT
            date_day,
            ROW_NUMBER() over (
                ORDER BY
                    date_day
            ) - 1 AS rn
        FROM
            crosschain.core.dim_dates
        WHERE
            date_day BETWEEN (
                SELECT
                    full_range_start_block_date
                FROM
                    full_range
            )
            AND (
                SELECT
                    full_range_end_block_date
                FROM
                    full_range
            )
    ),
    partition_block_counts AS (
        SELECT
            b.date_day AS block_date,
            COUNT(*) AS count_in_window
        FROM
            block_range b
            LEFT OUTER JOIN {{ source }}
            r
            ON r.block_timestamp :: DATE = b.date_day
        GROUP BY
            1
    ),
    range_groupings AS (
        SELECT
            block_date,
            count_in_window,
            conditional_change_event(
                count_in_window > 1
            ) over (
                ORDER BY
                    block_date
            ) AS group_val
        FROM
            partition_block_counts
    ),
    contiguous_ranges AS (
        SELECT
            MIN(block_date) AS start_block_date,
            MAX(block_date) AS end_block_date
        FROM
            range_groupings
        WHERE
            count_in_window > 1
        GROUP BY
            group_val
    ),
    between_stmts AS (
        SELECT
            CONCAT(
                'block_timestamp::date between \'',
                start_block_date,
                '\' and \'',
                end_block_date,
                '\''
            ) AS b
        FROM
            contiguous_ranges
    )
SELECT
    CONCAT('(', LISTAGG(b, ' OR '), ')')
FROM
    between_stmts {% endset %}
    debug() print (get_limits_query) {# {% set between_stmts = run_query(get_limits_query).columns [0].values() [0] %}
    {% if between_stmts != '()' %}
        /* in case empty update array */
        {% set predicate_override = between_stmts %}
    {% else %}
        {% set predicate_override = '1=1' %}
        /* need to have something or it will error since 'dynamic_block_date_ranges' is not a column */
    {% endif %}

    {{ return(predicate_override) }}
    #}
    {# {{ return(get_limits_query) }} #}
{% endmacro %}
