{{ config(
    materialized = 'view'
) }}

{{ fsc_utils.gha_task_performance_view() }}