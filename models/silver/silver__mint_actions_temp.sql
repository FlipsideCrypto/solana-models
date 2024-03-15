{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, event_type, mint)",
    incremental_strategy = 'delete+insert',
    incremental_predicates = ['block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

-- select dates of interest - none appear in inner_inst
WITH dates_filter AS (
    SELECT
        distinct(block_timestamp :: DATE) AS dt
    FROM
        {{ ref('silver__events') }}
    WHERE
        event_type IN (
        'mintTo',
        'initializeMint',
        'mintToChecked',
        'initializeMint2',
        'initializeConfidentialTransferMint',
        'initializeNonTransferableMint'
    )
        AND block_timestamp :: DATE >= '2023-01-01'
        AND program_id = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
),

base_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE 
    block_timestamp::date in (select dt from dates_filter)

),
prefinal as (
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    null as inner_index,
    event_type,
    instruction :parsed :info :mint :: STRING AS mint,
    instruction :parsed :info :account :: STRING as token_account, 
    instruction :parsed :info :decimals :: INTEGER AS DECIMAL,
    COALESCE(
        instruction :parsed :info :amount :: INTEGER,
        instruction :parsed :info :tokenAmount: amount :: INTEGER
    ) AS mint_amount,
    COALESCE(
        instruction :parsed :info :mintAuthority :: string,
        instruction :parsed :info :multisigMintAuthority :: string,
        instruction :parsed :info :authority :: string
    ) AS mint_authority,
    instruction :parsed :info :signers :: string AS signers,
    _inserted_timestamp
FROM
    base_events
WHERE
    event_type IN (
        'mintTo',
        'initializeMint',
        'mintToChecked',
        'initializeMint2',
        'initializeConfidentialTransferMint',
        'initializeNonTransferableMint'
    )
)
-- UNION
-- SELECT
--     block_id,
--     block_timestamp,
--     tx_id,
--     succeeded,
--     e.index,
--     i.index as inner_index,
--     i.value :parsed :type :: STRING AS event_type,
--     i.value :parsed :info :mint :: STRING AS mint,
--     i.value :parsed :info :account :: STRING as token_account, 
--     i.value :parsed :info :decimals :: INTEGER AS DECIMAL,
--     COALESCE(
--         i.value :parsed :info :amount :: INTEGER,
--         i.value :parsed :info :tokenAmount: amount :: INTEGER
--     ) AS mint_amount,
--     COALESCE(
--         i.value :parsed :info :mintAuthority :: string,
--         i.value :parsed :info :multisigMintAuthority :: string,
--         instruction :parsed :info :authority :: string
--     ) AS mint_authority,
--     i.value :parsed :info :signers :: string AS signers,
--     _inserted_timestamp
-- FROM
--     base_events e,
--     TABLE(FLATTEN(inner_instruction :instructions)) i
-- WHERE
--     i.value :parsed :type :: STRING IN (
--         'initializeConfidentialTransferMint',
--         'initializeNonTransferableMint'
--     )
-- )

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    event_type,
    mint,
    token_account,
    DECIMAL,
    mint_amount,
    CASE
        WHEN event_type IN (
            'initializeConfidentialTransferMint',
            'initializeNonTransferableMint'
        ) THEN FIRST_VALUE(mint_authority) ignore nulls over (
            PARTITION BY tx_id
            ORDER BY
                CASE
                    WHEN mint_authority IS NOT NULL THEN 0
                    ELSE 1
                END,
                INDEX
        )
        ELSE mint_authority
    END AS mint_authority,
    signers,
    _inserted_timestamp
FROM
    prefinal