{{ config (
    materialized = 'view'
) }}

SELECT
    VALUE
FROM
    {{ source(
        'bronze_streamline',
        'validator_metadata_api'
    ) }}
