{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['registry_metadata_id'],
    full_refresh = false,
    tags = ['scheduled_non_core']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'AU428Z7KbjRMjhmqWmQwUta2AvydbpfEZNBh8dStHTDi'
        AND _inserted_timestamp :: DATE >= '2024-06-21'
        AND event_type = 'create'

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
),
config_hash_data AS (
    SELECT
        *,
        decoded_instruction :args :confighash AS config_hash
    FROM
        base
),
ordered_config AS (
    SELECT
        tx_id,
        key :: INT AS key,
        VALUE :: INT AS VALUE
    FROM
        config_hash_data,
        LATERAL FLATTEN(
            input => config_hash
        )
),
sorted_config AS (
    SELECT
        tx_id,
        key,
        VALUE
    FROM
        ordered_config
    ORDER BY
        tx_id,
        key
),
combined_data AS (
    SELECT
        tx_id,
        ARRAY_CAT(ARRAY_CONSTRUCT(18, 32), ARRAY_AGG(VALUE) within GROUP (
    ORDER BY
        key ASC)) AS combined_array
    FROM
        sorted_config
    GROUP BY
        tx_id
),
hex_parts AS (
    SELECT
        tx_id,
        ARRAY_AGG(
            CASE
                WHEN LENGTH(REPLACE(utils.udf_int_to_hex(VALUE), '0x', '')) = 1 THEN CONCAT('0', REPLACE(utils.udf_int_to_hex(VALUE), '0x', ''))
                ELSE REPLACE(utils.udf_int_to_hex(VALUE), '0x', '')END
            ) AS hex_array
    FROM
        combined_data,
        LATERAL FLATTEN(
            input => combined_array
        )
    GROUP BY
        tx_id
),
hex_data AS (
    SELECT
        tx_id,
        CONCAT('0x', ARRAY_TO_STRING(hex_array, '')) AS hex_string
    FROM
        hex_parts
    GROUP BY
        tx_id,
        hex_array
),
ipfs_hash_data AS (
    SELECT
        tx_id,
        base.utils.udf_hex_to_base58(hex_string) AS ipfs_hash
    FROM
        hex_data
),
uri_calls AS (
    SELECT
        tx_id,
        CONCAT (
            'https://gateway.autonolas.tech/ipfs/',
            ipfs_hash
        ) AS token_uri_link,
        live.udf_api(token_uri_link) AS resp
    FROM
        ipfs_hash_data
),
response AS (
    SELECT
        A.resp,
        A.tx_id,
        b.block_id,
        b.program_id,
        b.service_id AS registry_id,
        A.token_uri_link,
        A.resp :data :attributes [0] :trait_type :: STRING AS trait_type,
        A.resp :data :attributes [0] :value :: STRING AS trait_value,
        REPLACE(
            A.resp :data :code_uri :: STRING,
            'ipfs://',
            'https://gateway.autonolas.tech/ipfs/'
        ) AS code_uri_link,
        A.resp :data :description :: STRING AS description,
        CASE
            WHEN A.resp :data :image :: STRING ILIKE 'ipfs://%' THEN REPLACE(
                A.resp :data :image :: STRING,
                'ipfs://',
                'https://gateway.autonolas.tech/ipfs/'
            )
            WHEN A.resp :data :image :: STRING NOT ILIKE '%://%' THEN CONCAT(
                'https://gateway.autonolas.tech/ipfs/',
                A.resp :data :image :: STRING
            )
            ELSE A.resp :data :image :: STRING
        END AS image_link,
        A.resp :data :name :: STRING AS NAME,
        b._inserted_timestamp
    FROM
        uri_calls A
        LEFT JOIN {{ ref('silver_olas__service_registrations') }}
        b
        ON A.tx_id = b.tx_id
)
SELECT
    resp,
    block_id,
    program_id,
    registry_id,
    token_uri_link,
    trait_type,
    trait_value,
    code_uri_link,
    description,
    image_link,
    NAME,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['program_id','registry_id']
    ) }} AS registry_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    response
WHERE
    resp :: STRING NOT ILIKE '%merkledag: not found%'
    AND resp :: STRING NOT ILIKE '%tuple index out of range%'
    AND resp :: STRING NOT ILIKE '%"error":%'
