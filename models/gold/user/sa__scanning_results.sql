{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(player_profile)'
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
    instruction :accounts [1] :: STRING AS player_profile,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority = 'C2478tbSLC1gfcDuCyr4pv66QQiybn77EiR1a4k7htT5'
            AND VALUE :parsed :type = 'transfer' THEN VALUE :parsed :info :amount
        END
    ) AS sdu_found,
    SUM(
        CASE
            WHEN VALUE :parsed :info :mint = 'foodQJAztMzX1DKpLaiounNe2BDMds5RNuPC6jsNrDG'
            AND VALUE :parsed :type = 'burn' THEN VALUE :parsed :info :amount
        END
    ) AS food_burned,
    SUM(
        CASE
            WHEN VALUE :parsed :info :mint = 'fueL3hBZjLLLJHiFH9cqZoozTG3XQZ53diwFPwbzNim'
            AND VALUE :parsed :type = 'burn' THEN VALUE :parsed :info :amount
        END
    ) AS fuel_burned,
    {{ dbt_utils.generate_surrogate_key(
        ['player_profile', 'date']
    ) }} AS mscanning_results_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__events') }}
    fe,
    LATERAL FLATTEN(
        input => inner_instruction :instructions
    )
WHERE
    program_id IN ('SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE')
    AND (
        VALUE :parsed :info :authority = 'C2478tbSLC1gfcDuCyr4pv66QQiybn77EiR1a4k7htT5'
        OR VALUE :parsed :info :mint IN (
            'foodQJAztMzX1DKpLaiounNe2BDMds5RNuPC6jsNrDG',
            'fueL3hBZjLLLJHiFH9cqZoozTG3XQZ53diwFPwbzNim'
        )
    )
    AND VALUE :parsed :type IN (
        'transfer',
        'burn'
    )
    AND instruction :accounts [18] = 'DataJpxFgHhzwu4zYJeHCnAv21YqWtanEBphNxXBHdEY'
    AND instruction :accounts [3] IN (
        SELECT
            wallet
        FROM
            {{ ref('sa__profile_wallets') }}
    )
    AND succeeded
    AND block_timestamp >= '2024-04-01'

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{ max_modified_timestamp }}'
{% endif %}
GROUP BY
    1,
    2
