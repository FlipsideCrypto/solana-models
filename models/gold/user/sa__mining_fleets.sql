{{ config(
    materialized = 'table',
    tags = ['daily']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_modified_query %}

SELECT
    MAX(modified_timestamp) AS modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query) [0] [0] %}
{% endif %}
{% endif %}
SELECT
    instruction :accounts [0] :: STRING AS wallet,
    instruction :accounts [1] :: STRING AS player_profile,
    instruction :accounts [3] :: STRING AS mining_fleets,
    {{ dbt_utils.generate_surrogate_key(
        ['wallet', 'player_profile', 'mining_fleets']
    ) }} AS mining_fleets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__events') }}
WHERE
    program_id IN ('SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE')
    AND instruction :accounts [18] = 'MineMBxARiRdMh7s1wdStSK4Ns3YfnLjBfvF5ZCnzuw'
    AND succeeded --= 'true'
    AND block_timestamp :: DATE >= CURRENT_DATE -30 {# {% if is_incremental() %}
    AND modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}

#}
GROUP BY
    1,
    2,
    3
