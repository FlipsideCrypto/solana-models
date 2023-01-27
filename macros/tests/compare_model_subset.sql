{% test compare_model_subset(model, compare_model, compare_columns, model_condition) %}

{% set compare_cols_csv = compare_columns | join(', ') %}

with a as (
    select {{compare_cols_csv}} from {{ model }}
    {{ model_condition }}
),
b as (
    select {{compare_cols_csv}} from {{ compare_model }}
),
a_minus_b as (
    select * from a
    except
    select * from b
),
b_minus_a as (
    select * from b
    except
    select * from a
),
unioned as (
    select 'in_actual_not_in_expected' as which_diff, a_minus_b.* from a_minus_b
    union all
    select 'in_expected_not_in_actual' as which_diff, b_minus_a.* from b_minus_a
)
select * from unioned

{% endtest %}