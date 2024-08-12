{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} },
    unique_key = ['fact_stake_pool_actions_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','action','stake_pool'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = [enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, address)'), 'ALTER TABLE {{this}} SET CHANGE_TRACKING = TRUE;'],
    tags = ['scheduled_non_core']
) }}

SELECT
    'lido' AS stake_pool_name,
    tx_id,
    block_id,
    block_timestamp,
    INDEX,
    succeeded,
    action,
    address,
    stake_pool,
    amount,
    'SOL' AS token,
    COALESCE (
        stake_pool_actions_lido_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','index']
        ) }}
    ) AS fact_stake_pool_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref(
        'silver__stake_pool_actions_lido'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    'socean' AS stake_pool_name,
    tx_id,
    block_id,
    block_timestamp,
    INDEX,
    succeeded,
    action,
    address,
    stake_pool,
    amount,
    'SOL' AS token,
    stake_pool_actions_socean_id AS fact_stake_pool_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__stake_pool_actions_socean_view'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    'eversol' AS stake_pool_name,
    tx_id,
    block_id,
    block_timestamp,
    INDEX,
    succeeded,
    action,
    address,
    stake_pool,
    amount,
    'SOL' AS token,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'index']
    ) }} AS fact_stake_pool_actions_id,
    '2000-01-01' AS inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref(
        'silver__stake_pool_actions_eversol_view'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    CASE
        WHEN stake_pool = '7ge2xKsZXmqPxa3YmXxXmzCp9Hc2ezrTxh6PECaxCwrL' THEN 'daopool'
        WHEN stake_pool = 'CtMyWsrUtAwXWiGr9WjHT5fC3p3fgV8cyGpLTo2LJzG1' THEN 'jpool'
        WHEN stake_pool = 'stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi' THEN 'blazestake'
        WHEN stake_pool = 'CgnTSoL3DgY9SFHxcLj6CgCgKKoTBr6tp4CPAEWy25DE' THEN 'cogent'
        WHEN stake_pool = 'LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZQti1xfWMLy6X' THEN 'laine'
    END AS stake_pool_name,
    tx_id,
    block_id,
    block_timestamp,
    INDEX,
    succeeded,
    action,
    address,
    stake_pool,
    amount,
    'SOL' AS token,
    COALESCE (
        stake_pool_actions_generic_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','index', 'inner_index']
        ) }}
    ) AS fact_stake_pool_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref(
        'silver__stake_pool_actions_generic'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    'marinade' AS stake_pool_name,
    tx_id,
    block_id,
    block_timestamp,
    INDEX,
    succeeded,
    action,
    address,
    stake_pool,
    amount,
    token,
    COALESCE (
        stake_pool_actions_marinade_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','index']
        ) }}
    ) AS fact_stake_pool_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref(
        'silver__stake_pool_actions_marinade'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    'jito' AS stake_pool_name,
    tx_id,
    block_id,
    block_timestamp,
    INDEX,
    succeeded,
    action,
    address,
    stake_pool,
    amount,
    'SOL' AS token,
    stake_pool_actions_jito_id AS fact_stake_pool_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'silver__stake_pool_actions_jito'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
