{{ config (
    materialized = 'table',
    tags = ['validator']
) }}

SELECT
    VALUE
FROM
    {{ source(
        'bronze_streamline',
        'validator_metadata_api'
    ) }}
