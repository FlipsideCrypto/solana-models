{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH burn_txs_sol_incinerator AS (

    SELECT
        DISTINCT tx_id,
        block_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'F6fmDVCQfvnEq2KR8hhfZSEczfM9JK9fWbCsYJNbTGn7'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-08-09'
{% endif %}
)
SELECT
    t.block_timestamp,
    block_id,
    t.tx_id,
    succeeded,
    instruction :accounts [1] :: STRING AS burner,
    instruction :accounts [2] :: STRING AS mint,
    _inserted_timestamp
FROM
    {{ ref('silver__events') }}
    t
    INNER JOIN burn_txs_sol_incinerator b
    ON t.tx_id = b.tx_id
WHERE
    t.program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND t.block_timestamp :: DATE >= '2022-08-09'
{% endif %}
UNION
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    COALESCE(
        instruction :parsed :info :authority :: STRING,
        instruction :parsed :info :multisigAuthority :: STRING
    ) AS burner,
    instruction :parsed :info :mint :: STRING AS mint,
    _inserted_timestamp
FROM
    {{ ref('silver__events') }}
WHERE
    event_type = 'burn'
    AND program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
    AND mint NOT IN (
        SELECT 
            DISTINCT from_mint 
        FROM {{ ref('silver__swaps') }}
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2020-10-11'
{% endif %}
