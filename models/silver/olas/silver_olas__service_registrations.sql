-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['service_registrations_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
    CREATE
    OR REPLACE temporary TABLE silver.service_registrations__intermediate_tmp AS

    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'AU428Z7KbjRMjhmqWmQwUta2AvydbpfEZNBh8dStHTDi'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND _inserted_timestamp :: DATE >= '2024-06-21'
{% endif %}

{% endset %}
{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.service_registrations__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.service_registrations__intermediate_tmp
),
create_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        signers[0]::string as signers,
        succeeded,
        program_id,
        event_type,
        decoded_instruction :args :agentids AS agent_ids_dict,
        decoded_instruction :args :serviceowner :: STRING AS owner_address,
        _inserted_timestamp
    FROM
        base
    WHERE
        event_type = 'create'
),
agent_ids AS (
    SELECT
        tx_id,
        ARRAY_AGG(
            VALUE :: INT
        ) AS agent_ids
    FROM
        create_events,
        LATERAL FLATTEN(
            input => agent_ids_dict
        )
    GROUP BY
        tx_id
),
deploy_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        signers,
        succeeded,
        program_id,
        decoded_instruction :args :multisig :: STRING AS multisig_address,
        decoded_instruction :args :serviceid :: INT AS service_id,
        decoded_instruction :accounts[1]:pubkey :: STRING AS owner_address,
        _inserted_timestamp
    FROM
        base
    WHERE
        event_type = 'deploy'
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.index,
    A.signers,
    A.succeeded,
    A.program_id,
    A.owner_address,
    a.event_type,
    b.agent_ids,
    C.multisig_address,
    C.service_id,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id','a.index']) }} AS service_registrations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    create_events A
    LEFT JOIN agent_ids b
    ON A.tx_id = b.tx_id
    LEFT JOIN deploy_events C
    ON A.owner_address = C.owner_address
