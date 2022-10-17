{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', signer, b_date)",
    incremental_strategy = 'delete+insert',
    cluster_by = 'signer'
) }}

WITH dates_changed AS (
    SELECT
        DISTINCT block_timestamp :: DATE AS block_timestamp_date
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    _inserted_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(
                DATEADD(
                    'day',
                    1,
                    COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                ),
                CURRENT_DATE - 1
            )
        FROM
            {{ this }}
    )
    AND (
        SELECT
            LEAST(
                DATEADD(
                    'day',
                    10,
                    COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                ),
                CURRENT_DATE - 1
            )
        FROM
            {{ this }}
    ) {% elif is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    _inserted_timestamp :: DATE = '2022-08-12'
{% endif %}
),
b AS (
    SELECT
        s.value :: STRING AS signer,
        block_timestamp :: DATE AS b_date,
        FIRST_VALUE(tx_id) over (
            PARTITION BY signer,
            b_date
            ORDER BY
                block_timestamp
        ) AS first_tx,
        LAST_VALUE(tx_id) over (
            PARTITION BY signer,
            b_date
            ORDER BY
                block_timestamp
        ) AS last_tx,*
    FROM
        {{ ref('silver__transactions') }}
        t,
        TABLE(FLATTEN(signers)) s
    WHERE
        b_date IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        )
    {% if is_incremental() and env_var(
        'DBT_IS_BATCH_LOAD',
        "false"
    ) == "true" %}
        AND _inserted_timestamp < (
            SELECT
                LEAST(
                    DATEADD(
                        'day',
                        11,
                        COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                    ),
                    CURRENT_DATE - 1
                )
            FROM
                {{ this }}
        )
    {% else %}
        AND _inserted_timestamp :: DATE = '2022-08-12'
    {% endif %}
),
C AS (
    SELECT
        tx_id,
        program_id,
        INDEX,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
    WHERE
        e.block_timestamp :: DATE IN (
            SELECT
                block_timestamp_date
            FROM
                dates_changed
        )
    {% if is_incremental() and env_var(
        'DBT_IS_BATCH_LOAD',
        "false"
    ) == "true" %}
        AND e._inserted_timestamp < (
            SELECT
                LEAST(
                    DATEADD(
                        'day',
                        11,
                        COALESCE(MAX(_inserted_timestamp :: DATE), '2022-08-12')
                    ),
                    CURRENT_DATE - 1
                )
            FROM
                {{ this }}
        )
    {% else %}
        AND e._inserted_timestamp :: DATE = '2022-08-12'
    {% endif %}
),
base_programs AS (
    SELECT
        tx_id,
        ARRAY_AGG(program_id) within GROUP (
            ORDER BY
                INDEX
        ) AS program_ids,
        program_ids [0] :: STRING AS first_program_id,
        program_ids [array_size(program_ids)-1] :: STRING AS last_program_id
    FROM
        C
    GROUP BY
        tx_id
),
first_last_programs AS (
    SELECT
        b.signer,
        b.b_date,
        b.tx_id,
        FIRST_VALUE(first_program_id) over (
            PARTITION BY signer,
            b_date
            ORDER BY
                block_timestamp
        ) AS first_program_id,
        LAST_VALUE(last_program_id) over (
            PARTITION BY signer,
            b_date
            ORDER BY
                block_timestamp
        ) AS last_program_id,
        LAST_VALUE(
            b._inserted_timestamp
        ) over (
            PARTITION BY signer,
            b_date
            ORDER BY
                block_timestamp
        ) AS _inserted_timestamp
    FROM
        b
        LEFT OUTER JOIN base_programs p
        ON p.tx_id = b.tx_id
),
final_programs AS (
    SELECT
        b.signer,
        b.b_date,
        b.first_program_id,
        b.last_program_id,
        array_union_agg(
            p.program_ids
        ) AS unique_program_ids,
        b._inserted_timestamp
    FROM
        first_last_programs b
        LEFT OUTER JOIN base_programs p
        ON p.tx_id = b.tx_id
    GROUP BY
        b.signer,
        b.b_date,
        b.first_program_id,
        b.last_program_id,
        b._inserted_timestamp
),
final_fees AS (
    SELECT
        signer,
        b_date,
        SUM(fee) AS total_fees
    FROM
        b
    WHERE
        INDEX = 0
    GROUP BY
        signer,
        b_date
),
final_num_txs AS (
    SELECT
        signer,
        b_date,
        first_tx,
        last_tx,
        COUNT(*) AS num_txs
    FROM
        b
    GROUP BY
        signer,
        b_date,
        first_tx,
        last_tx
)
SELECT
    s.*,
    f.total_fees,
    p.first_program_id,
    p.last_program_id,
    p.unique_program_ids,
    p._inserted_timestamp
FROM
    final_num_txs s
    LEFT OUTER JOIN final_fees f
    ON f.signer = s.signer
    AND f.b_date = s.b_date
    LEFT OUTER JOIN final_programs p
    ON p.signer = s.signer
    AND p.b_date = s.b_date
