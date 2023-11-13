{{ config(
    materialized='view',
    tags = ['scheduled_non_core']
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
FROM {{ ref('silver__labels') }}