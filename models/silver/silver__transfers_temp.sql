{{ config(
    materialized = 'incremental',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    full_refresh = false,
) }}

WITH base_transfers_i AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        INDEX::string as index,
        event_type,
        program_id,
        instruction,
        inner_instruction,
        succeeded,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
    event_type IN (
        'transferCheckedWithFee'
    ) -- dates of interest, events appears sporadically until 2023-11-11
          AND (
        --     block_timestamp :: DATE IN (
        --         '2023-01-09',
        --         '2023-03-07',
        --         '2023-05-24',
        --         '2023-05-28',
        --         '2023-05-31',
        --         '2023-07-12',
        --         '2023-07-20'
        --     )
            -- OR block_timestamp :: DATE between '2023-11-11' and '2024-01-01')
            -- block_timestamp :: DATE between '2024-01-02' and '2024-02-22')
            block_timestamp :: DATE > '2024-02-22')


    UNION
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        CONCAT(
            e.inner_instruction :index :: NUMBER,
            '.',
            ii.index
        ) AS INDEX,
        ii.value :parsed :type :: STRING AS event_type,
        ii.value :programId :: STRING AS program_id,
        ii.value as instruction,
        NULL AS inner_instruction,
        e.succeeded,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e,
        TABLE(FLATTEN(e.inner_instruction :instructions)) ii
    WHERE
        ii.value :parsed :type :: STRING IN (
        'transferCheckedWithFee'
        )
    and e.block_timestamp::date >= '2024-02-23' -- only appears in inner_inst at this date


),
base_post_token_balances AS (
    SELECT
        tx_id,
        owner,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }}
        
where (
            -- block_timestamp :: DATE IN (
            --     '2023-01-09',
            --     '2023-03-07',
            --     '2023-05-24',
            --     '2023-05-28',
            --     '2023-05-31',
            --     '2023-07-12',
            --     '2023-07-20'
            -- )
            -- OR block_timestamp :: DATE between '2023-11-11' and '2024-01-01')
            -- block_timestamp :: DATE between '2024-01-02' and '2024-02-22')
            block_timestamp :: DATE > '2024-02-22')

),
base_pre_token_balances AS (
    SELECT
        tx_id,
        owner,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___pre_token_balances') }}

where (
            -- block_timestamp :: DATE IN (
            --     '2023-01-09',
            --     '2023-03-07',
            --     '2023-05-24',
            --     '2023-05-28',
            --     '2023-05-31',
            --     '2023-07-12',
            --     '2023-07-20'
            -- )
            -- or block_timestamp :: DATE between '2023-11-11' and '2024-01-01')
            -- block_timestamp :: DATE between '2024-01-02' and '2024-02-22')
            block_timestamp :: DATE > '2024-02-22')
),
spl_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        e.program_id,
        e.succeeded,
        COALESCE(
            p.owner,
            e.instruction :parsed :info :authority :: STRING,
            e.instruction :parsed :info :multisigAuthority :: STRING
        ) AS tx_from,
        COALESCE(
            p2.owner,
            instruction :parsed :info :destination :: STRING
        ) AS tx_to,
        COALESCE(
            e.instruction :parsed :info :tokenAmount: decimals,
            p.decimal,
            p2.decimal,
            p3.decimal,
            p4.decimal,
            9 -- default to solana decimals
        ) AS decimal_adj,
        COALESCE (
            e.instruction :parsed :info :amount :: INTEGER,
            e.instruction :parsed :info :tokenAmount :amount :: INTEGER
        ) / pow(
            10,
            decimal_adj
        ) AS amount,
        COALESCE(
            p.mint,
            p2.mint,
            p3.mint,
            p4.mint
        ) AS mint,
        instruction :parsed :info :source :: STRING as source_token_account,
        instruction :parsed :info :destination :: STRING as dest_token_account,
        e._inserted_timestamp
    FROM
        base_transfers_i e
        LEFT OUTER JOIN base_pre_token_balances p
        ON e.tx_id = p.tx_id
        AND e.instruction :parsed :info :source :: STRING = p.account
        LEFT OUTER JOIN base_post_token_balances p2
        ON e.tx_id = p2.tx_id
        AND e.instruction :parsed :info :destination :: STRING = p2.account
        LEFT OUTER JOIN base_post_token_balances p3
        ON e.tx_id = p3.tx_id
        AND e.instruction :parsed :info :source :: STRING = p3.account
        LEFT OUTER JOIN base_pre_token_balances p4
        ON e.tx_id = p4.tx_id
        AND e.instruction :parsed :info :destination :: STRING = p4.account
    WHERE
        (
            e.instruction :parsed :info :authority :: STRING IS NOT NULL
            OR 
            e.instruction :parsed :info :multisigAuthority :: STRING IS NOT NULL
        )
),
sol_transfers AS (
    SELECT
        e.block_id,
        e.block_timestamp,
        e.tx_id,
        e.index,
        e.program_id,
        e.succeeded,
        instruction :parsed :info :source :: STRING AS tx_from,
        instruction :parsed :info :destination :: STRING AS tx_to,
        instruction :parsed :info :lamports / pow(
            10,
            9
        ) AS amount,
        'So11111111111111111111111111111111111111112' AS mint,
        NULL as source_token_account,
        NULL as dest_token_account,
        e._inserted_timestamp
    FROM
        base_transfers_i e
    WHERE
        instruction :parsed :info :lamports :: STRING IS NOT NULL
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    program_id,
    succeeded,
    INDEX,
    tx_from,
    tx_to,
    amount,
    mint,
    source_token_account,
    dest_token_account,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'index']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    spl_transfers
UNION
SELECT
    block_id,
    block_timestamp,
    tx_id,
    program_id,
    succeeded,
    INDEX,
    tx_from,
    tx_to,
    amount,
    mint,
    source_token_account,
    dest_token_account,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'index']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    sol_transfers
