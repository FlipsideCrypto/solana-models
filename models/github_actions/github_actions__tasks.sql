{{ config(
    materialized = 'view'
) }}

{{ fsc_utils.gha_tasks_view() }}