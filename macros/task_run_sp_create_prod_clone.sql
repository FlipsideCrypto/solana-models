{% macro task_run_sp_create_prod_clone(target_schema) -%}
    create or replace task {{target_schema}}.run_sp_create_prod_clone
        warehouse = dbt_cloud
        schedule = 'USING CRON 15 6 * * * UTC'
    as
        call {{ target_schema }}.create_prod_clone('solana', 'solana_dev', 'internal_dev')
{%- endmacro %}