{{ config(
      materialized='view'
    ) 
}}

SELECT
  blockchain, 
  creator, 
  address,
  label_type,
  label_subtype,
  project_name as label, 
  address_name as address_name
FROM {{ source(
        'crosschain',
        'address_labels'
    ) }} 
WHERE blockchain = 'solana'