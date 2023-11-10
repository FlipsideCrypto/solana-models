{{ config(
    materialized = 'table',
    unique_key = ['epoch']
) }}

SELECT
    ROW_NUMBER() over (
        ORDER BY
            SEQ4()
    ) - 1 :: INT AS epoch,
    CASE
        WHEN epoch = 0 THEN 1
        ELSE epoch * 432000
    END AS start_block,
    CASE
        WHEN epoch = 0 THEN 431999
        ELSE ((epoch + 1) * 432000) -1
    END AS end_block,
    {{ dbt_utils.generate_surrogate_key(['epoch']) }} AS epoch_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    TABLE(GENERATOR(rowcount => 5000))
