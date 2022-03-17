{{ config(
    materialized = 'view'
) }}

SELECT
    'magic eden v1' AS marketplace,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    sales_amount
FROM
    {{ ref('silver__nft_sales_magic_eden_v1') }}
UNION
SELECT
    'magic eden v2',
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    sales_amount
FROM
    {{ ref('silver__nft_sales_magic_eden_v2') }}
