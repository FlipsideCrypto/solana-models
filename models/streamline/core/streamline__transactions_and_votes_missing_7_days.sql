{{
    config(
        materialized = 'table',
    )
}}

SELECT
    *
FROM
    {{ source('solana_test_silver','transactions_and_votes_missing_7_days') }}
