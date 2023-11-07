{{ config(
    materialized = 'view'
) }}

{{ fsc_utils.gha_task_history_view() }}