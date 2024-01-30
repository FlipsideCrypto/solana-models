{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_block_date_ranges"],
    unique_key = "decoded_instructions_combined_id",
    cluster_by = ['program_id','block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}'
    ),
    merge_exclude_columns = ["inserted_timestamp"],
) }}
-- depends_on: {{ ref('silver__decoded_instructions') }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_inserted_timestamp = run_query(query).columns [0].values() [0] %}
{% endif %}
{% endif %}

{% if execute %}
    {% set query = """ CREATE OR REPLACE TEMPORARY TABLE silver.decoded_instructions__intermediate_tmp AS SELECT block_timestamp, block_id, tx_id, inner_index, program_id, decoded_instruction, decoded_instructions_id, _inserted_timestamp FROM """ ~ ref('silver__decoded_instructions') ~ """ WHERE block_timestamp::Date >= '2024-01-28'""" %}
    {% set incr = "" %}

{% if is_incremental() %}
{% set incr = """ AND _inserted_timestamp >= '{{ max_inserted_timestamp }}' """ %}
{% endif %}

{% do run_query(
    query ~ incr
) %}
{% endif %}

WITH txs AS (
    SELECT
        program_id,
        tx_id,
        signers
    FROM
        { ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= (
            SELECT
                MAX(
                    block_timestamp :: DATE
                )
            FROM
                silver.decoded_instructions__intermediate_tmp
        )
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    b.signers,
    A.inner_index,
    A.program_id,
    A.decoded_instruction,
    A._inserted_timestamp,
    decoded_instructions_id AS decoded_instructions_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    silver.decoded_instructions__intermediate_tmp A
    JOIN txs b
    ON A.tx_id = b.tx_id
    AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
