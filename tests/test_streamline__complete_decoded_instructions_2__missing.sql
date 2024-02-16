SELECT
    MD5(
        CAST(
            COALESCE(CAST(block_id AS text), '_dbt_utils_surrogate_key_null_') || '-' || COALESCE(CAST(tx_id AS text), '_dbt_utils_surrogate_key_null_') || '-' || COALESCE(CAST(INDEX AS text), '_dbt_utils_surrogate_key_null_') || '-' || COALESCE(CAST(inner_index AS text), '_dbt_utils_surrogate_key_null_') || '-' || COALESCE(CAST(program_id AS text), '_dbt_utils_surrogate_key_null_') AS text
        )
    ) AS complete_decoded_instructions_2_id
FROM
    {{ target.database }}.bronze.streamline_decoded_instructions_2
WHERE
    _partition_by_created_date_hour >= dateadd('hour',-3,date_trunc('hour',current_timestamp())) 
    AND _partition_by_created_date_hour < dateadd('hour',-2, date_trunc('hour',current_timestamp()))
EXCEPT
SELECT
    complete_decoded_instructions_2_id
FROM
    {{ ref('streamline__complete_decoded_instructions_2') }}
WHERE
    _inserted_timestamp >= dateadd('hour',-3,date_trunc('hour',current_timestamp()))
