{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}


with jupiter_dex_txs as (
    select 
        e.*, 
        coalesce(signers[1],signers[0])::string as swapper
    from {{ ref('silver__events') }} e
    INNER JOIN {{ ref('silver__transactions') }} t ON t.tx_id = e.tx_id and t.block_timestamp::date = e.block_timestamp::date
    WHERE program_id = 'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo'
        AND array_size(e.instruction:accounts) > 6
        AND e.block_id > 111442741
    {% if is_incremental() %}
    AND e.block_timestamp::date = '2022-03-07'
    -- AND e._inserted_timestamp >= (
    --     SELECT
    --         MAX(_inserted_timestamp)
    --     FROM
    --         {{ this }}
    -- )
    -- AND t._inserted_timestamp >= (
    --     SELECT
    --         MAX(_inserted_timestamp)
    --     FROM
    --         {{ this }}
    -- )
    {% else %}
    AND e.block_timestamp::date >= '2021-12-14'
    {% endif %}
),
base_transfers as (
    select *
    from {{ ref('silver__transfers2') }} tr
    {% if is_incremental() %}
    WHERE block_timestamp::date = '2022-03-07'
    -- WHERE _inserted_timestamp >= (
    --     SELECT
    --         MAX(_inserted_timestamp)
    --     FROM
    --         {{ this }}
    -- )
    {% else %}
    WHERE block_timestamp::date >= '2021-12-14'
    {% endif %}
),
base_post_token_balances as (
    select *
    from {{ ref('silver___post_token_balances') }}
    {% if is_incremental() %}
    WHERE block_timestamp::date = '2022-03-07'
    -- WHERE _inserted_timestamp >= (
    --     SELECT
    --         MAX(_inserted_timestamp)
    --     FROM
    --         {{ this }}
    -- )
    {% else %}
    WHERE block_timestamp::date >= '2021-12-14'
    {% endif %}
),
swaps_temp as(
    SELECT 
        a.block_id,
        a.block_timestamp,
        a.tx_id,
        COALESCE(
            split_part(INDEX::TEXT,'.',1)::int,
            INDEX::int
        ) AS index
        ,
        COALESCE(
            split_part(INDEX::TEXT,'.',2),
            Null
        ) AS inner_index,
        a.tx_from,
        a.tx_to,
        a.amount,
        a.mint,
        a.succeeded,
        a._inserted_timestamp
    FROM base_transfers as a
    WHERE a.tx_id in (SELECT tx_id from jupiter_dex_txs)
),
account_mappings as (
    select 
        tx_id,
        tx_to as associated_account,
        tx_from as owner
    from swaps_temp
    where amount = 0.00203928
    and mint = 'So11111111111111111111111111111111111111112'
    union 
    select
        tx_id,
        account AS associated_account,
        owner
    from base_post_token_balances
    union 
    select 
         tx_id,
        instruction:parsed:info:account::string as associated_account,
        coalesce(
            instruction:parsed:info:source::string,
            instruction:parsed:info:owner::string) as owner
    from SOLANA_DEV.silver.events
    where (program_id = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL' and event_type = 'create')
    or (program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' and event_type = 'closeAccount')
    {% if is_incremental() %}
    AND block_timestamp::date = '2022-03-07'
    -- AND _inserted_timestamp >= (
    --     SELECT
    --         MAX(_inserted_timestamp)
    --     FROM
    --         {{ this }}
    -- )
    {% else %}
    AND block_timestamp::date >= '2021-12-14'
    {% endif %}
),
swaps_w_destination AS (
    SELECT
        s.*,
        e.swapper,
        ii.value :parsed :info :destination :: STRING AS destination,
        ii.value :parsed :info :authority :: STRING AS authority,
        ii.value :parsed :info :source :: STRING AS source
    FROM
        swaps_temp s
    LEFT OUTER JOIN
        jupiter_dex_txs
        e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        destination IS NOT NULL
        AND s.inner_index = ii.index
        AND COALESCE(
            ii.value :programId :: STRING,
            ''
        ) <> '11111111111111111111111111111111'
),
swaps as(
    SELECT 
        d.*,
        min(d.inner_index) over (partition by d.tx_id, d.index) as min_inner_index,
        ROW_NUMBER() over (
                    PARTITION BY d.tx_id
                    ORDER BY
                        d.index,
                        d.inner_index
                ) AS rn,
        ROW_NUMBER() over (
                PARTITION BY d.tx_id, d.index
                ORDER BY
                    d.inner_index
            ) AS inner_rn
    from swaps_w_destination d
),
final_temp as (
    select s1.*, s2.mint as to_mint, s2.amount as to_amt
    from swaps s1 
    left outer join swaps s2 on s1.tx_id = s2.tx_id and s1.index = s2.index and s1.inner_index <> s2.inner_index
    left outer join account_mappings m on m.tx_id = s2.tx_id and m.associated_account = s2.destination
    where s1.inner_index = s1.min_inner_index 
    and s1.swapper in (s2.destination, m.owner)
    and s1.mint <> s2.mint
    union 
    select s1.*, null, null
    from swaps s1
    where s1.inner_index <> s1.min_inner_index
    and s1.tx_from = s1.swapper
    union 
    select 
        s1.block_id,
        s1.block_timestamp,
        s1.tx_id,
        s1.index,
        s1.inner_index,
        s1.tx_from,
        s1.tx_to,
        null as amount, 
        null as mint,
        s1.succeeded,
        s1._inserted_timestamp,
        null as swapper, 
        s1.destination,
        s1.authority,
        s1.source,
        s1.min_inner_index,
        s1.rn,
        s1.inner_rn,
        s1.mint as to_mint, 
        s1.amount as to_amt
    from swaps s1
    left outer join swaps s2 on s1.tx_id = s2.tx_id and s1.index = s2.index and s2.inner_index = s2.min_inner_index
    left outer join account_mappings m on m.tx_id = s1.tx_id and m.associated_account = s1.tx_to
    where s1.inner_index <> s1.min_inner_index
    and (m.owner = s1.swapper or s1.tx_to = s1.swapper)
    and s2.mint = s1.mint
)

SELECT 
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    swapper,
    mint as from_mint,
    amount as from_amt,
    to_mint,
    to_amt,
    _inserted_timestamp,
    ROW_NUMBER() over (
                PARTITION BY tx_id
                ORDER BY
                    rn
            ) AS swap_index
from final_temp
where coalesce(to_amt,0) > 0 or coalesce(from_amt,0) > 0