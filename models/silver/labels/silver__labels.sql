{{ config(
    materialized = 'incremental',
    unique_key = ['address','blockchain'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'modified_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address); DELETE FROM {{ this }} WHERE _is_deleted = TRUE;",
    tags=['scheduled_non_core_hourly'],
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
    _is_deleted,
    labels_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__labels') }}
WHERE
    1 = 1

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}