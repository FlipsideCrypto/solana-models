{{ config(
    materialized = 'view'
) }}

{{ fsc_utils.gha_task_schedule_view() }}