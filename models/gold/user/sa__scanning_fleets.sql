{{ config(
    materialized = 'incremental',
    unique_key = ['scanning_fleets_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(player_profile, scanning_fleets,scanning_fleets_id)'
    ),
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
    instruction :accounts [1] :: STRING AS player_profile,
    instruction :accounts [3] :: STRING AS scanning_fleets,
    {{ dbt_utils.generate_surrogate_key(
        [ 'player_profile', 'scanning_fleets']
    ) }} AS scanning_fleets_id,
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
    AND VALUE :parsed :info :authority = 'C2478tbSLC1gfcDuCyr4pv66QQiybn77EiR1a4k7htT5'
    AND instruction :accounts [18] = 'DataJpxFgHhzwu4zYJeHCnAv21YqWtanEBphNxXBHdEY'
    AND succeeded
    AND block_timestamp :: DATE >= '2024-04-01'

{% if is_incremental() %}
AND modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
GROUP BY
    1,
    2
