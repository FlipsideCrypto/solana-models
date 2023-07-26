-- depends_on: {{ ref('bronze__streamline_program_parser') }}
-- depends_on: {{ ref('bronze__streamline_FR_program_parser') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["tx_id", "index" ],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

SELECT
    b.block_timestamp,
    A.block_id,
    A.tx_id,
    COALESCE(
        A.index,
        VALUE :data :data [0] [0],
        VALUE :data [0] [0]
    ) :: INT AS INDEX,
    A.program_id,
    COALESCE(
        A.value :data :data [0] [1],
        A.value :data [1]
    ) AS decoded_instruction,
    A._inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_program_parser') }} A
{% else %}
    {{ ref('bronze__streamline_FR_program_parser') }} A
{% endif %}
JOIN {{ ref('silver__blocks') }}
b
ON A.block_id = b.block_id

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id, INDEX
ORDER BY
    A._inserted_timestamp DESC)) = 1
