{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('gov__fact_votes_agg_block') }}
