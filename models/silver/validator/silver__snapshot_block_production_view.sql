{{
    config(
        materialized = 'view',
    )
}}

SELECT
    *
FROM
    {{ source('solana_silver', 'snapshot_block_production') }}