{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} },
    tags = ['scheduled_non_core']
) }}

{% for model_suffix in ["socean","lido","eversol"] %}

    SELECT
        '{{ model_suffix }}' AS stake_pool_name,
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
         {{ 'stake_pool_actions_' ~ model_suffix ~ '_id' }},
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
            'silver__stake_pool_actions_' + model_suffix
        ) }}

        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
UNION ALL
SELECT
    CASE
        WHEN stake_pool = '7ge2xKsZXmqPxa3YmXxXmzCp9Hc2ezrTxh6PECaxCwrL' THEN 'daopool'
        WHEN stake_pool = 'CtMyWsrUtAwXWiGr9WjHT5fC3p3fgV8cyGpLTo2LJzG1' THEN 'jpool'
        WHEN stake_pool = 'stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi' THEN 'blazestake'
        WHEN stake_pool = 'Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb' THEN 'jito'
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
