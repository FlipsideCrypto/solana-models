{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(wallet)'
    ),
    tags = ['daily']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_modified_query %}

SELECT
    MAX(
        DATE
    ) AS bd
FROM
    {{ this }}

    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query) [0] [0] %}
{% endif %}
{% endif %}
SELECT
    block_timestamp :: DATE AS DATE,
    CASE
        WHEN signers [0] = 'FEEPAYye6DtaJhSXFR65ENXMroiseL1jqYT4QxHKRoZr' THEN signers [1]
        ELSE signers [0]
    END :: STRING AS wallet,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint = 'fueL3hBZjLLLJHiFH9cqZoozTG3XQZ53diwFPwbzNim'
            AND instruction :parsed :info :authority = signers [0] :: STRING
            AND instruction :parsed :type = 'transferChecked' THEN instruction :parsed :info :tokenAmount :uiAmount
            WHEN instruction :parsed :info :mint = 'fueL3hBZjLLLJHiFH9cqZoozTG3XQZ53diwFPwbzNim'
            AND instruction :parsed :info :authority != signers [0] :: STRING
            AND instruction :parsed :type = 'transferChecked' THEN instruction :parsed :info :tokenAmount :uiAmount * -1
        END
    ) AS fuel_refill,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint = 'foodQJAztMzX1DKpLaiounNe2BDMds5RNuPC6jsNrDG'
            AND instruction :parsed :info :authority = signers [0] :: STRING
            AND instruction :parsed :type = 'transferChecked' THEN instruction :parsed :info :tokenAmount :uiAmount
            WHEN instruction :parsed :info :mint = 'foodQJAztMzX1DKpLaiounNe2BDMds5RNuPC6jsNrDG'
            AND instruction :parsed :info :authority != signers [0] :: STRING
            AND instruction :parsed :type = 'transferChecked' THEN instruction :parsed :info :tokenAmount :uiAmount * -1
        END
    ) AS food_refill,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint = 'ammoK8AkX2wnebQb35cDAZtTkvsXQbi82cGeTnUvvfK'
            AND instruction :parsed :info :authority = signers [0] :: STRING
            AND instruction :parsed :type = 'transferChecked' THEN instruction :parsed :info :tokenAmount :uiAmount
            WHEN instruction :parsed :info :mint = 'ammoK8AkX2wnebQb35cDAZtTkvsXQbi82cGeTnUvvfK'
            AND instruction :parsed :info :authority != signers [0] :: STRING
            AND instruction :parsed :type = 'transferChecked' THEN instruction :parsed :info :tokenAmount :uiAmount * -1
        END
    ) AS ammo_refill,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint = 'tooLsNYLiVqzg8o4m3L2Uetbn62mvMWRqkog6PQeYKL'
            AND instruction :parsed :info :authority = signers [0] :: STRING THEN instruction :parsed :info :tokenAmount :uiAmount
            WHEN instruction :parsed :info :mint = 'tooLsNYLiVqzg8o4m3L2Uetbn62mvMWRqkog6PQeYKL'
            AND instruction :parsed :info :authority != signers [0] :: STRING THEN instruction :parsed :info :tokenAmount :uiAmount * -1
        END
    ) AS tool_refill,
    {{ dbt_utils.generate_surrogate_key(
        ['wallet', 'date']
    ) }} AS refill_results_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__events_inner') }}
WHERE
    instruction_program_id = 'FLEET1qqzpexyaDpqb2DGsSzE2sDCizewCg9WjrA6DBW'
    AND succeeded
    AND wallet IN (
        SELECT
            wallet
        FROM
            {{ ref('sa__profile_wallets') }}
    )
    AND block_timestamp >= '2024-04-01'

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{ max_modified_timestamp }}'
{% endif %}
GROUP BY
    1,
    2
