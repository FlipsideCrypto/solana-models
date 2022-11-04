{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }}}
) }}

{% for model_suffix in ["generic","socean","lido","marinade","eversol"] %}

    SELECT
        CASE
            WHEN '{{ model_suffix }}' = 'generic' THEN CASE
                WHEN stake_pool = '7ge2xKsZXmqPxa3YmXxXmzCp9Hc2ezrTxh6PECaxCwrL' THEN 'daopool'
                WHEN stake_pool = 'CtMyWsrUtAwXWiGr9WjHT5fC3p3fgV8cyGpLTo2LJzG1' THEN 'jpool'
                WHEN stake_pool = 'stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi' THEN 'blazestake'
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
        amount
    FROM
        {{ ref(
            'silver__stake_pool_actions_' + model_suffix
        ) }}

        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
