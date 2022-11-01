{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH jupiter_dex_txs AS (

    SELECT
        DISTINCT i.block_id,
        i.block_timestamp,
        i.tx_id,
        t.fee,
        t.succeeded,
        t.signers
    FROM
        {{ ref('silver___instructions') }}
        i
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = i.tx_id
    WHERE
        i.value :programId :: STRING = 'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo'
    AND i.block_timestamp::date between '2022-01-01' and '2022-03-28'
    AND t.block_timestamp::date between '2022-01-01' and '2022-03-28'


)

,

signers AS (
    SELECT
        t.tx_id,
        s.value :: STRING AS acct,
        s.index
    FROM
        jupiter_dex_txs t,
        TABLE(FLATTEN(t.signers)) s qualify(ROW_NUMBER() over (PARTITION BY t.tx_id
    ORDER BY
        s.index DESC)) = 1
)


,

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
    FROM {{ ref('silver__transfers2') }} as a
    WHERE a.block_timestamp::date between '2022-01-01' and '2022-03-28'
    and a.tx_id in (SELECT tx_id from jupiter_dex_txs)
)
,

swaps_w_destination AS (
    SELECT
        s.*,
        ii.value :parsed :info :destination :: STRING AS destination,
        ii.value :parsed :info :authority :: STRING AS authority,
        ii.value :parsed :info :source :: STRING AS source
    FROM
        swaps_temp s
    LEFT OUTER JOIN
        {{ ref('silver__events') }}
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
        AND e.program_id = 'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo'
        AND e.block_timestamp::date between '2022-01-01' and '2022-03-28'
    
)


,

swaps as(
    SELECT 
        d.*,
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
            ) AS inner_rn,
        s.acct as swapper
    from swaps_w_destination d
    left outer join signers s
        ON s.acct = d.authority
        AND s.tx_id = d.tx_id

)

-- SELECT * FROM swaps
-- WHERE TX_ID in ('3yVbSdBHnaoccDG4XpbTQrV1NNbqNiVdKYHio1iPNAnRpdnP1xK1sg6NdkdZKRhjj7tnve589UX3p55UBGWssTCD',
--                 '2hZuCGiMkuXxPJ4jZwKf8zzxzs4gUteDb28ZTFd5KnVpmfseP8YBWzRHuLP63iWiwUgb8mFM74o3cmJmag4YniKU',
--                 '5hr56Qdh5ZogtFBaoDPnLroEa7yVAfC6NjbbboNrpUGAoCWcgTtPSXe9Jdx8NhE85u1BAeUT1472C3R42YbDwJXj',
--                'nmN84qNUcZtGpCCxm1r8Qct3Z8vgPMRXFfd7QFohE28dVociEgbSA9xrkn44WgKa8GbTH86u3EiicCakCsbKiya',
--                '4Xdjhm8219RTPpfsi5tMtBKjbUNgm8Kh2kphNpH5vLqGFxFeh3dcntQT5nMZGFaD93BZWJwFtviB1afjWEdXZRsu',
--                '3c9M2C4mE3Q14PmiTMTHkQNMkzPRZSCdMXRc1oYooV8iiFJGLyfSMRgsZVWrXVWqsgvsaw6cgv21DXxCRamK8JHJ',
--                '5UexvqZu5dcQ3rN4PV2DuXStLu1aThf4hNs1gH1jEtZbV81XRWiCDUjzKJFNtUEB9dbv8Tu5NNAqzuB47U6EQQ3J')
-- ORDER BY tx_id, rn
,

swaps_refunds as (
    SELECT 
        s2.block_id,
        s2.block_timestamp,
        s2.tx_id,
        s2.succeeded,
        Null as swapper,
        Null as from_mint,
        Null as from_amt,
        s2.mint as to_mint,
        s2.amount as to_amt,
        s2._inserted_timestamp,
        s2.rn,
        s2.inner_rn
        
    FROM swaps as s1
    
    INNER JOIN swaps s2
        ON s1.tx_id = s2.tx_id
            AND s1.index = s2.index
            AND s1.swapper is not null
            and ((s1.tx_from = s2.tx_to and s1.mint = s2.mint) OR (s1.tx_from != s2.tx_to and s1.mint != s2.mint))
            
)
,


swaps_except_refunds as (
    SELECT 
        s1.block_id,
        s1.block_timestamp,
        s1.tx_id,
        s1.succeeded,
        s1.swapper,
        s1.mint as from_mint,
        s1.amount as from_amt,
        s2.mint as to_mint,
        s2.amount as to_amt,
        s1._inserted_timestamp,
        s1.rn,
        s1.inner_rn
        
    FROM swaps as s1
    
    INNER JOIN swaps s2
        ON s1.tx_id = s2.tx_id
        AND s1.tx_from = s2.tx_to
        AND s1.mint != s2.mint
        AND s1.index = s2.index
        AND s1.inner_rn = 1
),

final_temp as (
    SELECT * FROM swaps_except_refunds
    UNION
    SELECT * FROM swaps_refunds
)

SELECT 
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
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
from final_temp
