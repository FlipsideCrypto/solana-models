{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    project_name AS label,
    address_name AS address_name,
    COALESCE (
        labels_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address']
        ) }}
    ) AS dim_labels_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__labels') }}
