{{ config(
  materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    log_index,
    program_id,
    event_type,
    creator_fee,
    mm_fee,
    tswap_fee,
    sales_amount,
    _inserted_timestamp,
    nft_sales_tensorswap_buysellevent_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
  {{ source(
    'solana_silver',
    'nft_sales_tensorswap_buysellevent'
  ) }}
