{{ config(
    materialized = 'incremental',
    unique_key = "address",
    cluster_by = ['address'],
    tags = ['share']
) }}

SELECT
  blockchain, 
  creator, 
  address,
  label_type,
  label_subtype,
  label, 
  address_name
FROM {{ref('core__dim_labels')}}