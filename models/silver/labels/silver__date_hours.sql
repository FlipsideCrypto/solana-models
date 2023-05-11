{{ config(
    materialized = 'table'
) }}

SELECT
    *
FROM
    {{ ref('bronze__date_hours') }}
