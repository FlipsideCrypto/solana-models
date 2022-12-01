{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id","swap_index"],
    merge_predicates = ["DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from {{ this }}__dbt_tmp))"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base_events AS(

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id IN (
            --jupiter v4 program_ids
            'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB',
            --program ids for acct mapping
            'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
            'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        )

{% if is_incremental() %}
-- AND block_timestamp :: DATE = '2022-11-01'
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-07-12' -- first tx with v4
{% endif %}
),
dex_txs AS (
    SELECT
        e.*,
        signers [0] :: STRING AS swapper
    FROM
        base_events e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
        AND t.block_timestamp :: DATE = e.block_timestamp :: DATE
    WHERE
        program_id = 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'
        AND ARRAY_SIZE(
            e.instruction :accounts
        ) > 6
        AND inner_instruction_program_ids [0] <> 'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB' --associated with wrapping of tokens

{% if is_incremental() %}
-- AND t.block_timestamp :: DATE = '2022-11-01'
AND t._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND t.block_timestamp :: DATE >= '2022-07-12'
{% endif %}
),

temp_inner_program_ids as (
select *,
    solana_dev.silver.udf_get_jupv4_inner_programs(inner_instruction:instructions) as inner_programs
from dex_txs
),

temp_inner_program_ids_2 as (
    select 
        temp_inner_program_ids.*, 
        i.value:program_id::string as inner_swap_program_id, 
        i.value:inner_index::int as swap_program_inner_index_start,
        coalesce(lead(swap_program_inner_index_start) over 
                 (partition by tx_id, temp_inner_program_ids.index order by swap_program_inner_index_start)-1,999999)  as swap_program_inner_index_end
    from temp_inner_program_ids,
    table(flatten(inner_programs)) i
),

base_transfers AS (
    SELECT
        *
    FROM
        {{ ref('silver__transfers2') }}
        tr

{% if is_incremental() %}
WHERE
    -- block_timestamp :: DATE = '2022-11-01'
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    block_timestamp :: DATE >= '2022-07-12'
{% endif %}
),
base_post_token_balances AS (
    SELECT
        *
    FROM
        {{ ref('silver___post_token_balances') }}

{% if is_incremental() %}
WHERE
    -- block_timestamp :: DATE = '2022-11-01'
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    block_timestamp :: DATE >= '2022-07-12'
{% endif %}
),
swaps_temp AS(
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 1) :: INT, INDEX :: INT) AS INDEX,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 2), NULL) AS inner_index,
        A.program_id,
        A.tx_from,
        A.tx_to,
        A.amount,
        A.mint,
        A.succeeded,
        A._inserted_timestamp
    FROM
        base_transfers AS A
    WHERE
        A.tx_id IN (
            SELECT
                tx_id
            FROM
                dex_txs
        )
),
swap_w_inner_program_id as(
select s.*, t.inner_swap_program_id
from swaps_temp s
left outer join temp_inner_program_ids_2 t 
    on t.tx_id = s.tx_id 
    and t.index = s.index and s.inner_index between t.swap_program_inner_index_start and t.swap_program_inner_index_end
)
,
temp_acct_mappings as(
        SELECT
        tx_id,
        i.value :parsed :info :account :: STRING AS associated_account,
        COALESCE(
            i.value :parsed :info :source :: STRING,
            i.value :parsed :info :owner :: STRING
        ) AS owner
    FROM
        base_events
    LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        (
            (
                i.value :programId = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
                AND i.value :parsed :type = 'create'
            )
            OR (
                i.value :programId = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                AND i.value :parsed :type = 'closeAccount'
            )
        )

    UNION 
    SELECT
        tx_id,
        i.value :parsed :info :delegate :: STRING AS associated_account,
        i.value :parsed :info :owner :: STRING AS owner
    FROM
        base_events
    LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        (
            i.value :programId = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND i.value :parsed :type = 'approve'
        )

)
,
account_mappings AS (
    SELECT * FROM temp_acct_mappings
    union
    SELECT
        tx_id,
        tx_to AS associated_account,
        tx_from AS owner
    FROM
        swaps_temp
    WHERE
        amount = 0.00203928
        AND mint = 'So11111111111111111111111111111111111111112'
    UNION
    SELECT
        tx_id,
        account AS associated_account,
        owner
    FROM
        base_post_token_balances
    UNION
    SELECT
        tx_id,
        instruction :parsed :info :account :: STRING AS associated_account,
        COALESCE(
            instruction :parsed :info :source :: STRING,
            instruction :parsed :info :owner :: STRING
        ) AS owner
    FROM
        base_events
    WHERE
        (
            (
                program_id = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
                AND event_type = 'create'
            )
            OR (
                program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                AND event_type = 'closeAccount'
            )
        )
    UNION
    SELECT
        tx_id,
        instruction :parsed :info :delegate :: STRING AS associated_account,
        instruction :parsed :info :owner :: STRING AS owner
    FROM
        base_events
    WHERE
        (
            program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            AND event_type = 'approve'
        )
),
swaps_w_destination AS (
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.index,
        s.inner_index,
        COALESCE(
            m1.owner,
            s.tx_from
        ) AS tx_from,
        COALESCE(
            m2.owner,
            s.tx_to
        ) AS tx_to,
        s.amount,
        s.mint,
        s.succeeded,
        s._inserted_timestamp,
        e.swapper,
        e.program_id,
        s.inner_swap_program_id
    FROM
        swap_w_inner_program_id s
        LEFT OUTER JOIN dex_txs e
        ON s.tx_id = e.tx_id
        AND s.index = e.index
        LEFT OUTER JOIN account_mappings m1
        ON s.tx_id = m1.tx_id
        AND s.tx_from = m1.associated_account
        LEFT OUTER JOIN account_mappings m2
        ON s.tx_id = m2.tx_id
        AND s.tx_to = m2.associated_account
    WHERE
        s.program_id <> '11111111111111111111111111111111'
        and s.inner_swap_program_id <> 'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD'
),
swaps AS(
    SELECT
        d.*,
        ROW_NUMBER() over (
            PARTITION BY d.tx_id
            ORDER BY
                d.index,
                d.inner_index
        ) AS rn,
        ROW_NUMBER() over (
            PARTITION BY d.tx_id,
            d.index
            ORDER BY
                d.inner_index
        ) AS inner_rn
    FROM
        swaps_w_destination d
    WHERE
        d.swapper IS NOT NULL
        and d.tx_from <> d.tx_to
),
full_swaps_temp AS(
    SELECT
        s1.block_id,
        s1.block_timestamp,
        s1.tx_id,
        s1.succeeded,
        s1.program_id,
        s1.swapper,
        s1.mint,
        s1.amount,
        s1.rn,
        s1._inserted_timestamp,
        s1.index,
        s1.inner_index AS inner_index_1,
        s2.inner_index AS inner_index_2,
        s2.mint AS to_mint,
        s2.amount AS to_amt
    FROM
        swaps s1
        LEFT OUTER JOIN swaps s2
        ON s1.tx_id = s2.tx_id
        AND s1.index = s2.index
        AND s1.inner_index <> s2.inner_index
    WHERE
        s1.tx_to = s2.tx_from
        AND s1.swapper = s2.tx_to
        AND s1.mint <> s2.mint
),
final_temp AS(
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        program_id,
        swapper,
        mint as from_mint,
        amount as from_amt,
        rn,
        _inserted_timestamp,
        to_mint,
        to_amt
    FROM
        full_swaps_temp
    UNION
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.succeeded,
        s.program_id,
        s.swapper,
        s.mint as from_mint,
        s.amount as from_amt,
        s.rn,
        s._inserted_timestamp,
        NULL as to_mint,
        NULL as to_amt
    FROM
        swaps s
        LEFT JOIN full_swaps_temp f
        ON s.tx_id = f.tx_id
        AND s.index = f.index
        AND (
            s.inner_index = f.inner_index_1
            OR s.inner_index = f.inner_index_2
        )
    WHERE
        f.index IS NULL
        AND s.tx_from = s.swapper
    UNION
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.succeeded,
        s.program_id,
        s.swapper,
        NULL as from_mint,
        NULL as from_amt,
        s.rn,
        s._inserted_timestamp,
        s.mint as to_mint,
        s.amount as to_amt
    FROM
        swaps s
        LEFT JOIN full_swaps_temp f
        ON s.tx_id = f.tx_id
        AND s.index = f.index
        AND (
            s.inner_index = f.inner_index_1
            OR s.inner_index = f.inner_index_2
        )
    WHERE
        f.index IS NULL
        AND s.tx_to = s.swapper
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    swapper,
    from_mint,
    from_amt,
    to_mint,
    to_amt,
    _inserted_timestamp,
    ROW_NUMBER() over (
        PARTITION BY tx_id
        ORDER BY
            rn
    ) AS swap_index
FROM
    final_temp
WHERE
    COALESCE(
        to_amt,
        0
    ) > 0
    OR COALESCE(
        from_amt,
        0
    ) > 0