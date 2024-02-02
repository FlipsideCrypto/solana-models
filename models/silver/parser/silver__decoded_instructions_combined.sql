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
    tags = ['scheduled_non_core']
) }}
-- depends_on: {{ ref('silver__decoded_instructions') }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set max_inserted_query %}

SELECT
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query).columns [0].values() [0] %}
{% endif %}

{% set query = """ CREATE OR REPLACE TEMPORARY TABLE silver.decoded_instructions__intermediate_tmp AS SELECT block_timestamp, block_id, tx_id, index, inner_index, program_id, decoded_instruction, decoded_instructions_id, _inserted_timestamp FROM """ ~ ref('silver__decoded_instructions') %}
{% set incr = "" %}

{% if is_incremental() %}
{% set incr = """ WHERE _inserted_timestamp >= '""" ~ max_inserted_timestamp ~ """' """ %}
{% endif %}

{% do run_query(
    query ~ incr
) %}
{% set between_stmts = dynamic_block_date_ranges("silver.decoded_instructions__intermediate_tmp") %}
{% endif %}

WITH txs AS (
    SELECT
        block_timestamp,
        tx_id,
        signers,
        succeeded
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        {{ between_stmts }}
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.index,
    A.inner_index,
    b.signers,
    b.succeeded,
    A.program_id,
    A.decoded_instruction,
    A.decoded_instruction :name :: STRING AS event_type,
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
