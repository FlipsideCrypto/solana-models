-- depends_on: {{ ref('silver__mint_actions') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['decoded_metadata_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core','scheduled_non_core_hourly']
) }}

{% if execute %}
    {% set base_query %}
    CREATE
    OR REPLACE temporary TABLE silver.decoded_metadata__intermediate_tmp AS

    SELECT
        *
    FROM
        {{ ref('silver__mint_actions') }}
    WHERE
        event_type IN (
            'initializeMint',
            'initializeMint2'
        )
        AND succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND _inserted_timestamp :: DATE >= '2022-08-12'
{% endif %}

{% endset %}
{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.decoded_metadata__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH base_mints AS (
    SELECT
        *
    FROM
        silver.decoded_metadata__intermediate_tmp
),
base_decoded AS (
    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        succeeded
        AND program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        AND {{ between_stmts }}
),
metadata AS (
    SELECT
        *,
        silver.udf_get_account_pubkey_by_name(
            'mint',
            decoded_instruction :accounts
        ) :: STRING AS mint,
        decoded_instruction :args :createMetadataAccountArgsV3 :data :name :: STRING AS token_name,
        decoded_instruction :args :createMetadataAccountArgsV3 :data :symbol :: STRING AS symbol
    FROM
        base_decoded
    WHERE
        event_type = 'CreateMetadataAccountV3'
    UNION ALL
    SELECT
        *,
        silver.udf_get_account_pubkey_by_name(
            'mint',
            decoded_instruction :accounts
        ) :: STRING AS mint,
        decoded_instruction :args :createArgs :V1 :assetData :name :: STRING AS token_name,
        decoded_instruction :args :createArgs :V1 :assetData :symbol :: STRING AS symbol
    FROM
        base_decoded
    WHERE
        event_type = 'Create'
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.mint,
    A.decimal,
    b.token_name,
    b.symbol,
    a._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.mint']) }} AS decoded_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_mints A
    LEFT JOIN metadata b
    ON A.mint = b.mint
qualify(ROW_NUMBER() over (PARTITION BY a.mint ORDER BY a._inserted_timestamp DESC)) = 1

