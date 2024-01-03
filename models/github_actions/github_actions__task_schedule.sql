{{ config(
    materialized = 'view',
    tags = ['gha_tasks']
) }}

{{ fsc_utils.gha_task_schedule_view() }}