{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {% set predicate_override = "" %}
    {% if incremental_predicates[0] == "dynamic_block_date_ranges" %}
        -- run some queries to dynamically determine the min + max of this 'date_column' in the new data
        {% set get_limits_query %}
            WITH full_range AS (
                SELECT 
                    min(block_timestamp::date) as full_range_start_block_date,
                    max(block_timestamp::date) as full_range_end_block_date
                FROM {{ source }}
            ),
            block_range as (
                SELECT
                    date_day,
                    row_number() over (order by date_day) - 1 as rn
                FROM
                    crosschain.core.dim_dates
                WHERE 
                    date_day between (select full_range_start_block_date from full_range) and (select full_range_end_block_date from full_range)
            ),
            partition_block_counts as (
                SELECT
                    b.date_day as block_date,
                    COUNT(*) as count_in_window
                FROM
                    block_range b
                    left outer join {{ source }} r
                        on r.block_timestamp::date = b.date_day
                group by 1
            ),
            range_groupings AS (
                SELECT
                    block_date,
                    count_in_window,
                    CONDITIONAL_CHANGE_EVENT(count_in_window > 1) OVER (ORDER BY block_date) as group_val
                FROM
                    partition_block_counts
            ), 
            contiguous_ranges as (
                select 
                    min(block_date) as start_block_date,
                    max(block_date) as end_block_date
                from range_groupings
                where count_in_window > 1
                group by group_val
            ),
            between_stmts as (
                select 
                    concat('DBT_INTERNAL_DEST.block_timestamp::date between \'',start_block_date,'\' and \'',end_block_date,'\'') as b
                from 
                    contiguous_ranges
            )
            select 
                concat('(',listagg(b, ' OR '),')')
            from between_stmts
        {% endset %}
        {% set between_stmts = run_query(get_limits_query).columns[0].values()[0] %}
        {% if between_stmts != '()' %} /* in case empty update array */
            {% set predicate_override = between_stmts %}
        {% else %}
            {% set predicate_override = '1=1' %} /* need to have something or it will error since 'dynamic_block_date_ranges' is not a column */
        {% endif %}
    {% endif %}
    {% set predicates = [predicate_override] if predicate_override else incremental_predicates %}
    -- standard merge from here
    {% set merge_sql = dbt.get_merge_sql(target, source, unique_key, dest_columns, predicates) %}
    {{ return(merge_sql) }}

{% endmacro %}