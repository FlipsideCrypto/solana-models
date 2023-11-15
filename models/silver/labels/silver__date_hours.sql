{{ config(
    materialized = 'table',
    tags=['scheduled_non_core'],
) }}

SELECT
    *
FROM
    {{ ref('bronze__date_hours') }}
