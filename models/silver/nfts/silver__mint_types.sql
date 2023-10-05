{{ config(
    materialized = 'incremental'
) }}

WITH initialization AS (

    SELECT
        *,
        CASE
            WHEN DECIMAL = 0 THEN 'nft'
            WHEN DECIMAL > 0 THEN 'token'
            ELSE 'unknown'
        END AS mint_type
    FROM
        solana.silver.mint_actions
    WHERE
        event_type IN (
            'initializeMint',
            'initializeMint2'
        )
        AND succeeded
),
metaplex_events AS (
    SELECT
        tx_id,
        block_timestamp,
        INDEX,
        0 AS inner_index,
        program_id,
        e.instruction :accounts AS accounts,
        ARRAY_SIZE(accounts) AS num_accounts
    FROM
        solana.silver.events e
    WHERE
        program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        AND succeeded
        AND block_timestamp::date > '2022-06-01'
    UNION ALL
    SELECT
        tx_id,
        e.block_timestamp,
        e.index,
        i.index AS inner_index,
        i.value :programId AS program_id,
        i.value :accounts AS accounts,
        ARRAY_SIZE(accounts) AS num_accounts
    FROM
        solana.silver.events e,
        TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        i.value :programId :: STRING = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        AND succeeded
        AND block_timestamp::date > '2022-06-01'
),
metaplex_mint_events AS (
    SELECT
        *,
        CASE
            WHEN num_accounts = 9
            AND accounts [6] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [7] = '11111111111111111111111111111111'
            AND accounts [8] = 'SysvarRent111111111111111111111111111111111' THEN 'Create Master Edition'
            WHEN num_accounts = 8
            AND accounts [6] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [7] = '11111111111111111111111111111111' THEN 'Create Master Edition V3'
            WHEN num_accounts = 11
            AND accounts [9] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [10] = '11111111111111111111111111111111'
            AND accounts [11] = 'SysvarRent111111111111111111111111111111111' THEN 'Create Master Edition Deprecated'
            WHEN num_accounts = 9
            AND accounts [6] = '11111111111111111111111111111111'
            AND accounts [7] = 'Sysvar1nstructions1111111111111111111111111'
            AND accounts [8] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' THEN 'Create'
            WHEN num_accounts IN (
                13,
                14
            )
            AND accounts [11] :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND accounts [12] :: STRING = '11111111111111111111111111111111' THEN 'Edition'
            WHEN num_accounts = 6
            AND accounts [5] = '11111111111111111111111111111111' THEN 'Create Metadata Account V3'
            WHEN num_accounts = 7
            AND accounts [5] = '11111111111111111111111111111111'
            AND accounts [6] = 'SysvarRent111111111111111111111111111111111' THEN 'Create Metadata Account'
        END AS metaplex_event_type
    FROM
        metaplex_events
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                CASE
                    WHEN metaplex_event_type IN (
                        'Create Master Edition Deprecated',
                        'Create Master Edition V3',
                        'Create Master Edition'
                    ) THEN 1
                    WHEN metaplex_event_type = 'Edition' THEN 2
                    WHEN metaplex_event_type IN (
                        'Create Metadata account',
                        'Create',
                        'Create Metadata Account V3'
                    ) THEN 3
                    ELSE 4
                END
        ) AS rn
    FROM
        metaplex_mint_events
),
nonfungibles AS (
    SELECT
        A.tx_id,
        A.mint,
        A.decimal,
        A.mint_type,
        CASE
            WHEN b.metaplex_event_type = 'Edition'
            AND A.decimal = 0 THEN 'Edition'
            WHEN b.metaplex_event_type IN (
                'Create Master Edition Deprecated',
                'Create Master Edition V3',
                'Create Master Edition'
            )
            AND A.decimal = 0 THEN 'NonFungible'
            WHEN b.metaplex_event_type IN ('Create')
            AND accounts [1] <> 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
            AND A.decimal = 0 THEN 'NonFungible'
        END AS mint_standard_type
    FROM
        initialization A
        LEFT JOIN ranked b
        ON A.tx_id = b.tx_id
    WHERE
        b.rn = 1
        AND mint_standard_type IS NOT NULL
),
fungibles_and_others AS (
    SELECT
        A.tx_id,
        A.mint,
        A.decimal,
        A.mint_type,
        CASE
            WHEN b.metaplex_event_type IN ('Create')
            AND accounts [1] = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
            AND A.decimal = 0 THEN 'FungibleAsset'
            WHEN b.metaplex_event_type IN (
                'Create Metadata Account',
                'Create',
                'Create Metadata Account V3'
            )
            AND A.decimal = 0 THEN 'FungibleAsset'
            WHEN b.metaplex_event_type IN (
                'Create Metadata Account',
                'Create',
                'Create Metadata Account V3'
            )
            AND A.decimal > 0 THEN 'Fungible'
            ELSE 'unknown'
        END AS mint_standard_type
    FROM
        initialization A
        LEFT JOIN ranked b
        ON A.tx_id = b.tx_id
        AND b.rn = 1
    WHERE
        A.tx_id NOT IN (
            SELECT
                tx_id
            FROM
                nonfungibles
        )
    GROUP BY
        1,
        2,
        3,
        4,
        5
)
SELECT
    *
FROM
    nonfungibles
UNION ALL
SELECT
    *
FROM
    fungibles_and_others
