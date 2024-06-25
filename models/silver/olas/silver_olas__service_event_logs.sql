-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['service_event_logs_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
    CREATE
    OR REPLACE temporary TABLE silver.service_event_logs__intermediate_tmp AS

    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'SMPLecH534NA9acpos4G6x7uf3LWbCAwZQE9e8ZekMu'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND _inserted_timestamp :: DATE >= '2024-06-21'
    AND block_timestamp :: DATE >= '2023-07-07'
{% endif %}

{% endset %}
{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.service_event_logs__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.service_event_logs__intermediate_tmp
),
multisigs AS (
    SELECT
        multisig_address
    FROM
        {{ ref('silver_olas__service_registrations') }}
),
olas_decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        signers,
        succeeded,
        program_id,
        silver.udf_get_account_pubkey_by_name(
            'multisig',
            decoded_instruction :accounts
        ) AS multisig_address,
        event_type,
        _inserted_timestamp
    FROM
        base
    WHERE
        multisig_address IN (
            SELECT
                multisig_address
            FROM
                multisigs
        )
),
base_events AS (
    SELECT
        A.*
    FROM
        {{ ref('silver__events') }} A
        INNER JOIN (
            SELECT
                DISTINCT tx_id,
                block_timestamp :: DATE AS block_date
            FROM
                olas_decoded
        ) d
        ON d.block_date = A.block_timestamp :: DATE
        AND d.tx_id = A.tx_id
    WHERE
        program_id = 'SMPLecH534NA9acpos4G6x7uf3LWbCAwZQE9e8ZekMu'
        AND {{ between_stmts }}
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.index,
    A.inner_index,
    A.signers [0] :: STRING AS signer,
    A.succeeded,
    A.program_id,
    A.multisig_address,
    A.event_type,
    c.service_id,
    -- b.instruction,
    -- b.inner_instruction,
    b.instruction :data :: STRING AS DATA,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id','a.index','a.inner_index']) }} AS service_event_logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    olas_decoded A
    LEFT JOIN base_events b
    ON A.tx_id = b.tx_id
    AND A.index = b.index
    LEFT JOIN {{ ref('silver_olas__service_registrations') }} c
    ON a.multisig_address = c.multisig_address
