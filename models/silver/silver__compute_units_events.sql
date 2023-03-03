{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    signers,
    succeeded,
    instructions,
    inner_instructions,
    log_messages,
    silver.udf_get_compute_units_consumed(log_messages) as compute_consumed,
    silver.udf_get_compute_units_total(log_messages) as compute_available,
    _inserted_timestamp
FROM
    {{ ref('silver__transactions') }}
    t
WHERE 
    1 = 1
{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic(
        this,
        30,
        '2023-02-23'
    ) }}
{% endif %}
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-30'
    AND '2022-09-05'
{% endif %}

--AND tx_id = '5CZ9kgU1aiEmifq8DaC1gLfNmq1NnQYEJbYAZwrMQNriwvMyjaoqwPxNG7bGYExD756xLcVjozFhPhbCuhj992yn' -- Kellen's Transaction ID
