-- depends_on: {{ ref('bronze__streamline_decoded_instructions_2') }}
-- depends_on: {{ ref('bronze__streamline_FR_decoded_instructions_2') }}
{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_block_date_ranges"],
    unique_key = "decoded_instructions_id",
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE','program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core'],
) }}

/* run incremental timestamp value first then use it as a static value */
{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT
                MAX(_inserted_timestamp) as _inserted_timestamp
            FROM
                {{ this }}
        {% endset %}

        {% set max_inserted_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

SELECT
    b.block_timestamp,
    A.block_id,
    A.tx_id,
    COALESCE(
        A.index,
        VALUE :data :data [0] [0],
        VALUE :data [0] [0]
    ) :: INT AS INDEX,
    A.inner_index,
    A.program_id,
    COALESCE(
        A.value :data :data [0] [1],
        A.value :data [1],
        A.value :data
    ) AS decoded_instruction,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['A.tx_id', 'A.index', 'A.inner_index']
    ) }} AS decoded_instructions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_decoded_instructions_2') }} A
{% else %}
    {{ ref('bronze__streamline_FR_decoded_instructions_2') }} A
{% endif %}
JOIN {{ ref('silver__blocks') }}
b
ON A.block_id = b.block_id

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= '{{ max_inserted_timestamp }}'
AND 
    A._partition_by_created_date_hour between dateadd('hour', -2, date_trunc('hour','{{ max_inserted_timestamp }}'::timestamp_ntz)) and dateadd('hour',1,date_trunc('hour','{{ max_inserted_timestamp }}'::timestamp_ntz))
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id, INDEX, coalesce(inner_index,-1)
ORDER BY
    A._inserted_timestamp DESC)) = 1
