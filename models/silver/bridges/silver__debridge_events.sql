{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['debridge_api']
) }}

-- WITH base AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        program_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            program_id = 'src5qyZHqTqecJV4aY6Cb6zDZLMDzrDKKezs22MPHr4'
            OR (
                program_id = 'dst5MGcFPoBeREFAA5E3tU5ij8m5uVYwkzkSAbsLbNo'
                AND signers [0] = instruction :accounts [1] :: STRING
            )
        )
        AND block_timestamp :: DATE = '2023-12-25'
        AND succeeded
-- {% if is_incremental() %}
-- and
--     _inserted_timestamp >= (
--         SELECT
--             MAX(_inserted_timestamp)
--         FROM
--             {{ this }}
--     )
-- {% endif %}
-- )

