{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH base_table AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        program_id,
        instruction :accounts [0] :: STRING AS purchaser,
        instruction :accounts [2] :: STRING AS seller,
        instruction :accounts [5] :: STRING AS mint,
        inner_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
    WHERE
        program_id = 'HYPERfwdTjyJ2SCaKHmpF2MtrXqWxrsotYDsTrshHWq8'
        AND (instruction :accounts [10] :: STRING = '5pdaXth4ijgDCeYDKgSx3jAbN7m8h4gy1LRCErAAN1LM'
        OR instruction :accounts [11] :: STRING = '5pdaXth4ijgDCeYDKgSx3jAbN7m8h4gy1LRCErAAN1LM')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-09-22' -- no Hyperspace sales before this DATE
{% endif %}
),
sale_amt AS (
    SELECT
        b.tx_id,
        SUM(
            i.value :parsed :info :lamports
        ) / POW(
            10,
            9
        ) :: NUMBER AS sales_amount
    FROM
        base_table b
        LEFT JOIN TABLE (FLATTEN(inner_instruction :instructions)) i
    WHERE
        i.value :parsed :type :: STRING = 'transfer'
        AND i.value :program :: STRING = 'system'
    GROUP BY
        b.tx_id
)
SELECT
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.succeeded,
    b.program_id,
    b.purchaser,
    b.seller,
    b.mint,
    sales_amount,
    b._inserted_timestamp
FROM
    sale_amt e
    INNER JOIN base_table b
    ON e.tx_id = b.tx_id
