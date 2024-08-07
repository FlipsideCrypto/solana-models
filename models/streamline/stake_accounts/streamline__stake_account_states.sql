{{ config(
    materialized = 'table',
    unique_key = ['account_address'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['streamline_stake_accounts']
) }}

{% set stake_program_address = 'Stake11111111111111111111111111111111111111' %}

WITH base_stake_account_events AS (
    /* 
        This first select is a very special case...
        It seems this account's creation / delegation event is listed as "FAILED" on very old blocks (646291, 646008)
        and there are no subsequent successful creation / delegations
        I can't find another case of this but looking on explorers the data is the same...
        It has an active delegation but the creation/delegation event is from a failed TX
        The code below is to manually include this one account so that it is marked as `is_active=TRUE`
    */
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        index,
        -1 AS inner_index,
        event_type,
        CASE
            WHEN event_type = 'merge' THEN
                instruction:parsed:info:source::string
            WHEN event_type = 'split' THEN
                instruction:parsed:info:newSplitAccount::string
            ELSE
                instruction:parsed:info:stakeAccount::string
        END AS account_address,
        FROM
            {{ ref('silver__events') }}
        WHERE 
            program_id = '{{ stake_program_address }}'
            AND instruction:parsed:info:stakeAccount::string = '7BUrMkGxj7weJQLZf8yNLSQUUA1uWxwr6tkMx4wn9pzn'
    UNION ALL
    SELECT 
        block_id,
        block_timestamp,
        tx_id,
        index,
        -1 AS inner_index,
        event_type,
        CASE
            WHEN event_type = 'merge' THEN
                instruction:parsed:info:source::string
            WHEN event_type = 'split' THEN
                instruction:parsed:info:newSplitAccount::string
            ELSE
                instruction:parsed:info:stakeAccount::string
        END AS account_address,
    FROM
        {{ ref('silver__events') }}
    WHERE 
        program_id = '{{ stake_program_address }}'
        AND succeeded
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        e.index,
        i.index AS inner_index,
        i.value:parsed:type::string AS inner_event_type,
        CASE
            WHEN inner_event_type = 'merge' THEN
                i.value:parsed:info:source::string
            WHEN inner_event_type = 'split' THEN
                i.value:parsed:info:newSplitAccount::string
            ELSE
                i.value:parsed:info:stakeAccount::string
        END AS account_address,
    FROM
        {{ ref('silver__events') }} e
    JOIN
        TABLE(flatten(e.inner_instruction:instructions)) i 
    WHERE 
        array_contains('{{ stake_program_address }}'::variant, inner_instruction_program_ids)
        AND i.value :programId :: STRING = '{{ stake_program_address }}'
        AND succeeded
)
SELECT 
    block_id,
    block_timestamp,
    account_address,
    event_type NOT IN ('deactivate','deactivateDelinquent','merge') AS is_active,
    event_type = 'delegate' AS is_delegated,
FROM
    base_stake_account_events
WHERE
    event_type IN ('initialize','delegate','deactivate','deactivateDelinquent','split','merge')
QUALIFY
    row_number() OVER (PARTITION BY account_address ORDER BY block_id DESC, index DESC, inner_index DESC) = 1
