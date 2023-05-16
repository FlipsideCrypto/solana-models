{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'crosschain',
        'dim_date_hours'
    ) }}