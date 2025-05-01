{{ config(
    materialized = 'table',
    unique_key = ['dim_epoch_id']
) }}

SELECT
    epoch,
    start_block,
    end_block,
    epoch_id as dim_epoch_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp
from
  {{ ref('silver__epoch') }}
