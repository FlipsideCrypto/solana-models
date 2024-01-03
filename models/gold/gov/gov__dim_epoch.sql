{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

SELECT
    epoch,
    start_block,
    end_block,
    epoch_id as dim_epoch_id,
    modified_timestamp,
    inserted_timestamp
from
  {{ ref('silver__epoch') }}
