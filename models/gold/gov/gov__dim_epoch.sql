{{ config(
    materialized = 'view',
    post_hook = 'ALTER VIEW {{this}} SET CHANGE_TRACKING = TRUE;',
    tags = ['scheduled_non_core']
) }}

SELECT
    epoch,
    start_block,
    end_block,
    epoch_id AS dim_epoch_id,
    modified_timestamp,
    inserted_timestamp
FROM
    {{ ref('silver__epoch') }}
