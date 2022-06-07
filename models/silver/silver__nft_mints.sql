{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, index, mint, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH mint_tx AS (

    SELECT
        DISTINCT t.tx_id,
        t.signers [0] :: STRING AS signer,
        CASE
            WHEN ARRAY_SIZE(
                t.signers
            ) > 1 THEN t.signers [1] :: STRING
            ELSE NULL
        END AS potential_nft_mint,
        t.succeeded
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
    WHERE
        event_type IN ('mintTo', 'initializeMint')

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
AND t.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
txs AS (
    SELECT
        e.block_timestamp,
        e.block_id,
        t.tx_id,
        t.succeeded AS succeeded,
        t.signer,
        t.potential_nft_mint,
        program_id,
        e.index,
        i.index AS inner_index,
        COALESCE(
            i.value :parsed :info :lamports :: INTEGER,
            i.value :parsed :info :amount :: INTEGER
        ) AS sales_amount,
        LAST_VALUE(
            i.value :parsed :info :mint :: STRING ignore nulls
        ) over (
            PARTITION BY t.tx_id,
            e.index
            ORDER BY
                inner_index
        ) AS nft,
        LAST_VALUE(
            i.value :parsed :info :multisigAuthority :: STRING ignore nulls
        ) over (
            PARTITION BY t.tx_id,
            e.index
            ORDER BY
                inner_index
        ) AS wallet,
        i.value :parsed :info :authority :: STRING AS authority,
        i.value :parsed :info :source :: STRING AS source,
        i.value: parsed :info :destination :: STRING AS destination,
        case -- marindate specific
            when e.inner_instruction :instructions [0] :programId :: STRING = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' then
                e.instruction:accounts[3]::string
            else 
                null
        end as update_authority,
        e.ingested_at
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN mint_tx t
        ON t.tx_id = e.tx_id
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        e.event_type IS NULL
        AND (
            ARRAY_CONTAINS(
                'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' :: variant,
                e.instruction :accounts :: ARRAY
            )
            OR e.inner_instruction :instructions [0] :programId :: STRING = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        )
        AND t.succeeded = TRUE

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
mint_currency AS (
    SELECT
        DISTINCT t.tx_id,
        p.mint AS mint_paid,
        p.account,
        p.decimal
    FROM
        txs t
        INNER JOIN {{ ref('silver___post_token_balances') }}
        p
        ON t.tx_id = p.tx_id
    WHERE
        source = p.account

{% if is_incremental() %}
AND p.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    block_timestamp,
    block_id,
    t.tx_id,
    succeeded,
    program_id,
    INDEX,
    COALESCE(
        wallet,
        signer
    ) AS purchaser,
    case
        when update_authority = '6jG2QcwaJPFS8Y9SzgH2kfKPj6ERhLi9RVtH8kRahj4j' then -- marinade nfts are "free"
            0
        else 
            SUM(sales_amount / pow(10, COALESCE(p.decimal, 9))) 
    end AS mint_price,
    COALESCE(
        p.mint_paid,
        'So11111111111111111111111111111111111111111'
    ) AS mint_currency,
    COALESCE(
        potential_nft_mint,
        nft
    ) AS mint,
    ingested_at
FROM
    txs t
    LEFT OUTER JOIN mint_currency p
    ON p.tx_id = t.tx_id
    AND p.account = t.source
WHERE
    sales_amount IS NOT NULL
    AND destination IS NOT NULL
GROUP BY
    block_timestamp,
    block_id,
    t.tx_id,
    program_id,
    INDEX,
    purchaser,
    succeeded,
    mint_currency,
    mint,
    update_authority,
    ingested_at
