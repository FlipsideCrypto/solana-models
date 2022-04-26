{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
) }}

WITH post_token_balances AS (

    SELECT
        tx_id,
        account,
        mint,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }} 
    WHERE
        mint = 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey'
{% if is_incremental() %}
AND ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
marinade_lock_txs AS (
    SELECT DISTINCT
        tx_id
    FROM
        {{ ref('silver__events') }} 
        e,
        TABLE(FLATTEN(input => inner_instruction :instructions, outer => true)) ii
    WHERE
        program_id = 'tovt1VkTE2T4caWoeFP6a2xSFoew5mNpd7FWidyyMuk'
    {% if is_incremental() %}
    AND e.ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
    EXCEPT
    SELECT DISTINCT
        tx_id
    FROM
        {{ ref('silver__events') }} 
        e,
        TABLE(FLATTEN(input => inner_instruction :instructions, outer => true)) ii
    WHERE
        program_id = 'Govz1VyoyLD5BL6CSCxUJLVLsQHRwjfFj1prNsdNg5Jw' -- ignore votes
    {% if is_incremental() %}
    AND e.ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
),
b as (
    select 
        t.tx_id,
        t.succeeded,
        l.index,
        l.value::string as log_message,
        case when l.value::string like '%invoke%' then
            1
        when l.value::string like '%success' then
            -1
        else 
            0
        end as cnt,
        sum(cnt) over (partition by t.tx_id order by l.index rows between unbounded preceding and current row) as event_cumsum    
    FROM
            {{ ref('silver__transactions') }}  t
    INNER JOIN marinade_lock_txs d on t.tx_id = d.tx_id
    LEFT OUTER JOIN TABLE(FLATTEN(t.log_messages)) l
    {% if is_incremental() %}
    WHERE t.ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
)
, c as (
    select b.*,
        lag(event_cumsum,1) over (partition by tx_id order by index) as prev_event_cumsum
    from b
)
, tx_logs as (
    select c.tx_id,
        c.succeeded,
        c.index as log_index,
        c.log_message,
        CASE
            WHEN c.log_message = 'Program log: Instruction: CreateSimpleNftEscrow' THEN 'MINT LOCK'
            WHEN c.log_message = 'Program log: Instruction: UpdateLockAmount' THEN 'UPDATE LOCK'
            ELSE NULL
        END AS action,
        conditional_true_event(
            prev_event_cumsum = 0
        ) over (
            PARTITION BY tx_id
            ORDER BY
                index
        ) AS event_index
    from c
)
, actions_tmp as (
    select 
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        l.succeeded,
        e.index,
        e.instruction:parsed:info:amount*pow(10,-9)::float as lock_amount,
        coalesce(e.instruction:accounts[5]::string,e.instruction:parsed:info:authority::string) as locker,
        coalesce(e.instruction:accounts[6]::string,e.instruction:parsed:info:destination::string) as locker_account_tmp,
        last_value(action ignore nulls) over (partition by e.tx_id order by e.index) as main_action,
        min(e.index) over (partition by e.tx_id order by e.index) as min_index,
        case when main_action = 'MINT LOCK' then e.instruction:accounts[2]::string
        else NULL end as locker_nft,
        e.instruction
    from {{ ref('silver__events') }}  e
    inner join marinade_lock_txs m on m.tx_id = e.tx_id
    left outer join tx_logs l on e.tx_id = l.tx_id and e.index = l.event_index and l.action is not null
    where (e.event_type = 'transfer' or l.action is not null)
    {% if is_incremental() %}
    and e.ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
)
select
    a1.block_timestamp,
    a1.block_id,
    a1.tx_id,
    coalesce(a1.succeeded,a2.succeeded) as succeeded,
    coalesce(a1.locker,a2.locker) as signer,
    coalesce(a1.locker_account_tmp,a2.locker_account_tmp) as locker_account,
    last_value(coalesce(a1.locker_nft,a2.locker_nft) ignore nulls) over (partition by locker_account order by a1.block_timestamp::date) as locker_nft,
    b.mint,
    case when a1.main_action LIKE '% LOCK' then 'LOCK' end as action,
    coalesce(a1.lock_amount,a2.lock_amount) as lock_amount
from actions_tmp a1
left outer join actions_tmp a2 on a1.tx_id = a2.tx_id and a1.index <> a2.index
left outer join post_token_balances b on a1.tx_id = b.tx_id and coalesce(a1.locker_account_tmp,a2.locker_account_tmp) = b.account
where a1.index = a1.min_index