{{ config(
    materialized = 'view'
) }}

SELECT
    m.name,
    m.description,
    m.registry_id,
    m.program_id,
    CASE
        WHEN m.program_id = 'AU428Z7KbjRMjhmqWmQwUta2AvydbpfEZNBh8dStHTDi' THEN 'Service'
    END AS registry_type,
    m.trait_type,
    m.trait_value,
    m.code_uri_link,
    m.image_link,
    s.agent_ids,
    m.registry_metadata_id,
    m.inserted_timestamp,
    GREATEST(
        COALESCE(
            m.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ),
        COALESCE(
            s.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver_olas__registry_metadata') }}
    m
    LEFT JOIN {{ ref('silver_olas__service_registrations') }}
    s
    ON m.registry_id = s.service_id