{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    _id AS block_id
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            MAX(block_id)
        FROM
            {{ ref('streamline__chainhead') }}
    )