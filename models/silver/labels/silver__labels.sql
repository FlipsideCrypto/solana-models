{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    merge_exclude_columns = ["inserted_timestamp"],
    tags=['scheduled_non_core'],
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__labels') }}
WHERE
    1 = 1

{% if is_incremental() %}
AND insert_date >= (
    SELECT
        MAX(
            insert_date
        )
    FROM
        {{ this }}
)
{% endif %}