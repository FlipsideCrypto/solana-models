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
    tx_to AS wallet,
    SUM(
        CASE
            WHEN mint = 'fueL3hBZjLLLJHiFH9cqZoozTG3XQZ53diwFPwbzNim' THEN amount
            ELSE 0
        END
    ) AS harvest_fuel,
    SUM(
        CASE
            WHEN mint = 'foodQJAztMzX1DKpLaiounNe2BDMds5RNuPC6jsNrDG' THEN amount
            ELSE 0
        END
    ) AS harvest_food,
    SUM(
        CASE
            WHEN mint = 'ammoK8AkX2wnebQb35cDAZtTkvsXQbi82cGeTnUvvfK' THEN amount
            ELSE 0
        END
    ) AS harvest_ammo,
    SUM(
        CASE
            WHEN mint = 'tooLsNYLiVqzg8o4m3L2Uetbn62mvMWRqkog6PQeYKL' THEN amount
            ELSE 0
        END
    ) AS harvest_tool,
    {{ dbt_utils.generate_surrogate_key(
        ['wallet', 'date']
    ) }} AS refill_results_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__transfers') }}
WHERE
    tx_from IN ('6gxMWRY4DJnx8WfJi45KqYY1LaqMGEHfX9YdLeQ6Wi5')
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
