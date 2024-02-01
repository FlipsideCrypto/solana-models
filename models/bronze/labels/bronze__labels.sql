{{ config(
      materialized='view'
    ) 
}}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    _is_deleted,
    labels_combined_id
FROM  {{ source(
        'crosschain_silver',
        'labels_combined'
    ) }}
WHERE blockchain = 'solana'