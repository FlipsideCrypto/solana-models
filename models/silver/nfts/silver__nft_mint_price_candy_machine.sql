{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', mint, payer, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH base_candy_machine_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        (
            program_id IN (
                'cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ',
                'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ'
            )
            OR (
                program_id = 'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR'
                AND ARRAY_SIZE(
                    instruction :accounts
                ) > 15
            )
        )
        AND succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-08-28'
{% endif %}
),
base_ptb AS (
    SELECT
        DISTINCT mint AS mint_paid,
        account,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    block_timestamp :: DATE >= '2021-08-28'
{% endif %}
),
candy_machine AS (
    SELECT
        e.block_timestamp,
        e.tx_id,
        e.index,
        i.index AS inner_index,
        e.program_id,
        CASE
            WHEN e.program_id = 'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR'
            AND instruction :accounts [13] = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' THEN instruction :accounts [4] :: STRING
            ELSE instruction :accounts [5] :: STRING
        END AS mint,
        CASE
            WHEN e.program_id = 'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR' THEN instruction :accounts [3] :: STRING
            ELSE instruction :accounts [2] :: STRING
        END AS payer,
        COALESCE(
            i.value :parsed :info :lamports :: INTEGER,
            i.value :parsed :info :amount :: INTEGER
        ) AS transfer_amount,
        CASE
            WHEN i.value :parsed :info :lamports IS NOT NULL THEN NULL
            ELSE i.value :parsed :info :source
        END AS token_account,
        e._inserted_timestamp
    FROM
        base_candy_machine_events e
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
pre_final AS (
    SELECT
        e.*,
        COALESCE(
            p.mint_paid,
            'So11111111111111111111111111111111111111111'
        ) AS mint_currency,
        COALESCE(
            p.decimal,
            9
        ) AS DECIMAL
    FROM
        candy_machine e
        LEFT OUTER JOIN base_ptb p
        ON e.token_account = p.account
)
SELECT
    p.mint,
    p.payer,
    mint_currency,
    DECIMAL,
    p.program_id,
    SUM(p.transfer_amount / pow(10, DECIMAL)) AS mint_price,
    array_unique_agg(tx_id) AS tx_ids,
    MAX(block_timestamp) AS block_timestamp,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    pre_final p
WHERE
    p.mint NOT IN (
        'Sysvar1nstructions1111111111111111111111111',
        'SysvarRent111111111111111111111111111111111'
    ) -- not mint events
GROUP BY
    1,
    2,
    3,
    4,
    5
