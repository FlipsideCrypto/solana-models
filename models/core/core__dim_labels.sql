{{ config(
      materialized='view'
    ) 
}}

SELECT
  blockchain, 
  creator, 
  address,
  l1_label as label_type,
  l2_label as label_subtype,
  project_name as label, 
  address_name as address_name
FROM {{ source(
        'crosschain',
        'address_labels'
    ) }} 
WHERE blockchain = 'solana'