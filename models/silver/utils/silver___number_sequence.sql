{{ config(
    materialized = 'table',
    cluster_by = 'round(_id,-3)'
) }}

SELECT
    ROW_NUMBER() over (
        ORDER BY
            SEQ4()
    ) - 1 :: INT AS _id
FROM
    TABLE(GENERATOR(rowcount => 50000000))