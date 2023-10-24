{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }}}
) }}

{% for model_suffix in ["generic","socean","lido","eversol"] %}

    SELECT
        CASE
            WHEN '{{ model_suffix }}' = 'generic' THEN CASE
                WHEN stake_pool = '7ge2xKsZXmqPxa3YmXxXmzCp9Hc2ezrTxh6PECaxCwrL' THEN 'daopool'
                WHEN stake_pool = 'CtMyWsrUtAwXWiGr9WjHT5fC3p3fgV8cyGpLTo2LJzG1' THEN 'jpool'
                WHEN stake_pool = 'stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi' THEN 'blazestake'
                WHEN stake_pool = 'Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb' THEN 'jito'
                WHEN stake_pool = 'CgnTSoL3DgY9SFHxcLj6CgCgKKoTBr6tp4CPAEWy25DE' THEN 'cogent'
                WHEN stake_pool = 'LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZQti1xfWMLy6X' THEN 'laine'
            END
            ELSE '{{ model_suffix }}'
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
        'SOL' as token
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
        'marinade' as stake_pool_name,
        tx_id,
        block_id,
        block_timestamp,
        INDEX,
        succeeded,
        action,
        address,
        stake_pool,
        amount,
        token
    FROM
        {{ ref(
            'silver__stake_pool_actions_marinade'
        ) }}
