-- depends_on: {{ ref('silver__events') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    tags = ['idls','scheduled_non_core']
) }}

{% if execute %}
    {% set create_base_query %}
        CREATE OR REPLACE TEMPORARY TABLE base__dbt_tmp AS 
        SELECT
            program_id,
            idl,
            SHA2(PARSE_JSON(idl)) AS idl_hash,
            discord_username,
            _inserted_timestamp
        FROM
            {{ source(
                "crosschain_public",
                "user_idls"
            ) }}
        WHERE
            blockchain = 'solana'
            AND NOT is_duplicate

        {% if is_incremental() %}
        AND _inserted_timestamp > (
            SELECT
                COALESCE(
                    MAX(
                        _inserted_timestamp
                    ),
                    '1970-01-01'
                )
            FROM
                {{ this }}
        )
        {% endif %}
        ORDER BY
            _inserted_timestamp ASC
        LIMIT
            10
    {% endset %}
    {% do run_query(create_base_query) %}
    {% set program_ids = run_query("""select program_id from base__dbt_tmp""").columns[0].values() %}
    {% if program_ids|length == 0 %}
        {% set program_ids = ["abc"] %}
    {% endif %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        base__dbt_tmp
), program_requests AS (
    {% for program_id in program_ids %}
        SELECT
            e.program_id,
            OBJECT_CONSTRUCT(
                'tx_id',
                e.tx_id,
                'block_id',
                e.block_id,
                'index',
                e.index,
                'program_id',
                e.program_id,
                'instruction',
                e.instruction,
                'is_verify',
                TRUE
            ) AS request
        FROM
            {{ ref('silver__events') }} e
        WHERE succeeded
        AND block_timestamp >= CURRENT_DATE - 30 
        AND e.program_id = '{{ program_id }}'
        LIMIT 100
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
),
groupings AS (
    SELECT
        program_id,
        ARRAY_AGG(request) AS requests
    FROM
        program_requests
    GROUP BY
        1
),
responses AS (
    SELECT
        program_id,
        streamline.udf_verify_idl(requests) AS response
    FROM
        groupings
),
results as (
    select 
        program_id,
        response :status_code :: INTEGER as status_code,
        try_parse_json(response:body)::array as decoded_instructions
    from responses
),
expanded as (
    select
        r.program_id,
        r.status_code,
        iff(coalesce(d.value:error::string,'') = '' and coalesce(d.value:data:error::string,'') = '' and status_code = 200,false,true) is_error
    from results r,
    table(flatten(decoded_instructions)) d
),
program_error_rates as (
    select 
        program_id,
        count_if(is_error)/count(*) as error_rate
    from expanded
    group by program_id
),
pre_final as (
    SELECT
        b.program_id,
        b.idl,
        b.idl_hash,
        (r.error_rate <= 0.25) as new_is_valid,
        b.discord_username,
        b._inserted_timestamp,
        CONCAT(
            b.program_id,
            '-',
            b.idl_hash
        ) AS id
    FROM
        program_error_rates r
        JOIN base b
        ON b.program_id = r.program_id
        LEFT OUTER JOIN {{ this }} t 
        ON b.program_id = t.program_id
    WHERE 
        (
            t.program_id is NULL -- brand new
            OR 
            (
                t.idl_hash <> b.idl_hash -- updated
                AND new_is_valid -- only update if the new one is valid
            )
        )
    qualify(ROW_NUMBER() over(PARTITION BY b.program_id
    ORDER BY
        b._inserted_timestamp DESC)) = 1
)
SELECT 
    program_id,
    idl,
    idl_hash,
    new_is_valid as is_valid,
    discord_username,
    _inserted_timestamp,
    id
FROM pre_final