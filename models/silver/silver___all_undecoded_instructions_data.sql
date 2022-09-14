{{ config(
    materialized = 'view',
    post_hook = 'call silver.sp_bulk_get_decoded_instructions_data()',
) }}

WITH create_validator_gauge_instruction_data AS (

    SELECT
        program_id,
        'CreateValidatorGauge' AS instruction_type,
        tx_id,
        INDEX,
        instruction :data :: STRING AS DATA
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'va12L6Z9fa5aGJ7gxtJuQZ928nySAk5UetjcGPve3Nu'
        AND DATA LIKE 'ykMtAXE4%'
),
marinade_gauge_set_vote_instruction_data AS (
    SELECT
        program_id,
        'GaugeSetVote' AS instruction_type,
        tx_id,
        INDEX,
        instruction :data :: STRING AS DATA
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'tovt1VkTE2T4caWoeFP6a2xSFoew5mNpd7FWidyyMuk'
        AND DATA LIKE 'XGC1Gnw6V9Q%'
),
possible_undecoded_instructions AS (
    SELECT
        *
    FROM
        create_validator_gauge_instruction_data
    UNION
    SELECT
        *
    FROM
        marinade_gauge_set_vote_instruction_data
)
SELECT
    p.*
FROM
    possible_undecoded_instructions p
    LEFT OUTER JOIN {{ source(
        'bronze_streamline',
        'decoded_instructions_data_api'
    ) }}
    d
    ON p.tx_id = d.tx_id
    AND p.index = d.event_index
WHERE
    d.tx_id IS NULL
