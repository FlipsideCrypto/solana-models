-- depends_on: {{ ref('bronze__streamline_decoded_instructions_2') }}
{{ config (
    materialized = "incremental",
    incremental_predicates = ['DBT_INTERNAL_DEST._inserted_timestamp::date >= LEAST(current_date-2,(select min(_inserted_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    unique_key = "complete_decoded_instructions_2_id",
    cluster_by = ["ROUND(block_id, -3)","program_id"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(complete_decoded_instructions_2_id)'),
    tags = ['streamline_decoder'],
) }}

/* run incremental timestamp value first then use it as a static value */
{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT
                COALESCE(MAX(_inserted_timestamp),'2000-01-01'::timestamp_ntz) _inserted_timestamp
            FROM
                {{ this }}
        {% endset %}

        {% set max_inserted_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

SELECT
    block_id,
    tx_id,
    index,
    inner_index,
    program_id,
    {{ dbt_utils.generate_surrogate_key(['block_id','tx_id','index','inner_index','program_id']) }} as complete_decoded_instructions_2_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_decoded_instructions_2') }}
WHERE
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
AND 
    _partition_by_created_date_hour >= dateadd('hour', -1, date_trunc('hour','{{ max_inserted_timestamp }}'::timestamp_ntz))
{% else %}
    {{ ref('bronze__streamline_FR_decoded_instructions_2') }}
{% endif %}
qualify(ROW_NUMBER() over (PARTITION BY complete_decoded_instructions_2_id
ORDER BY
    _inserted_timestamp DESC)) = 1 