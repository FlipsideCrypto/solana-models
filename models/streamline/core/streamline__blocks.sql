{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    _id AS block_number
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            MAX(block_number)
        FROM
            {{ ref('streamline__chainhead') }}
    )