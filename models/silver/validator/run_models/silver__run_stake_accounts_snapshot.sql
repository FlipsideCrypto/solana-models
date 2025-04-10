{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    tags = ['scheduled_non_core']
) }}

WITH table_epoch AS (
    SELECT
        MAX(epoch) AS current_table_epoch
    FROM
        {{ ref('gov__fact_stake_accounts') }}
),

epoch_call as (
        SELECT
            live.udf_api(
                'POST',
                '{service}/{Authentication}',
                OBJECT_CONSTRUCT(
                    'Content-Type',
                    'application/json'
                ),
                OBJECT_CONSTRUCT(
                    'id',
                    current_timestamp,
                    'jsonrpc',
                    '2.0',
                    'method',
                    'getEpochInfo',
                    'params',
                    []
                ),
                'Vault/prod/solana/quicknode/mainnet'
            ) AS data
    ),

sol_epoch AS (
    SELECT
        DATA:data:result:epoch::INT AS current_sol_epoch
    FROM
        epoch_call
),

run_model AS (
    SELECT
        table_epoch.current_table_epoch,
        sol_epoch.current_sol_epoch,
        github_actions.workflow_dispatches(
            'FlipsideCrypto',
            'solana-models',
            'dbt_run_streamline_stake_accounts_snapshot.yml',
            NULL
        ) AS run_stake_accounts_snapshot
    FROM
        table_epoch,
        sol_epoch
    WHERE
        sol_epoch.current_sol_epoch > table_epoch.current_table_epoch
)


SELECT
    dummy,
    COALESCE(
        current_sol_epoch,
        0
    ) AS current_sol_epoch,
    COALESCE(
        current_table_epoch,
        0
    ) AS current_table_epoch,
    COALESCE(
        run_stake_accounts_snapshot,
        OBJECT_CONSTRUCT(
            'status', 
            'skipped'
        )
    ) AS run_stake_accounts_snapshot,
    SYSDATE() AS test_timestamp
FROM
    (SELECT 1 AS dummy)
    LEFT JOIN run_model ON 1 = 1