{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, index, mint, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH mint_tx_tmp AS (

    SELECT
        t.tx_id,
        t.signers [0] :: STRING AS signer,
        CASE
            WHEN ARRAY_SIZE(
                t.signers
            ) > 1 THEN t.signers [1] :: STRING
            ELSE NULL
        END AS potential_nft_mint,
        t.succeeded,
        silver.udf_get_all_inner_instruction_events(
            inner_instruction :instructions
        ) AS inner_instruction_events
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN {{ ref('silver__transactions') }}
        t
        ON t.tx_id = e.tx_id
    WHERE
        (event_type IN ('mintTo', 'initializeMint')
        OR (program_id = 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'
        AND (ARRAY_CONTAINS('mintTo' :: variant, inner_instruction_events)
        OR ARRAY_CONTAINS('initializeMint' :: variant, inner_instruction_events))))

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
AND t.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
mint_tx AS (
    SELECT
        DISTINCT tx_id,
        signer,
        potential_nft_mint,
        succeeded
    FROM
        mint_tx_tmp
),
txs_tmp AS (
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
        CASE
            -- marindate specific
            WHEN e.inner_instruction :instructions [0] :programId :: STRING = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' THEN e.instruction :accounts [3] :: STRING
            ELSE NULL
        END AS update_authority,
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
            OR e.program_id IN (
                'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s',
                'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'
            )
        )
        AND t.succeeded = TRUE

{% if is_incremental() %}
AND e.ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
txs AS (
    SELECT
        *
    FROM
        txs_tmp
    WHERE
        (
            program_id NOT IN (
                '342zaQ1jgejKvPMqcnZejuZ845NtnfkpGvo9j15gDmEL',
                'TTTi5K2DS4qD95yyvvWht53qFWT8ff1Hp3xKsrp1QQf'
            )
            OR (
                program_id IN (
                    '342zaQ1jgejKvPMqcnZejuZ845NtnfkpGvo9j15gDmEL',
                    'TTTi5K2DS4qD95yyvvWht53qFWT8ff1Hp3xKsrp1QQf'
                )
                AND wallet IS NULL
            )
        )
),
transfers AS (
    SELECT
        tr.*
    FROM
        {{ ref('silver__transfers') }}
        tr
        INNER JOIN mint_tx t
        ON t.tx_id = tr.tx_id

{% if is_incremental() %}
WHERE
    tr.ingested_at :: DATE >= CURRENT_DATE - 2
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
),
pre_final AS (
    SELECT
        block_timestamp,
        block_id,
        t.tx_id,
        succeeded,
        program_id,
        CASE
            WHEN program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' THEN COALESCE(
                signer,
                wallet
            )
            ELSE COALESCE(
                wallet,
                signer
            )
        END AS purchaser,
        CASE
            WHEN update_authority = '6jG2QcwaJPFS8Y9SzgH2kfKPj6ERhLi9RVtH8kRahj4j' THEN -- marinade nfts are "free"
            0
            ELSE SUM(sales_amount / pow(10, COALESCE(p.decimal, 9)))
        END AS mint_price,
        COALESCE(
            p.mint_paid,
            'So11111111111111111111111111111111111111111'
        ) AS mint_currency,
        COALESCE(
            nft,
            potential_nft_mint
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
        purchaser,
        succeeded,
        mint_currency,
        mint,
        update_authority,
        ingested_at
),
pre_pre_final AS (
    SELECT
        pf.block_timestamp,
        pf.block_id,
        pf.tx_id,
        pf.program_id,
        pf.purchaser,
        pf.succeeded,
        CASE
            WHEN tr.tx_id IS NOT NULL
            AND mint_currency = 'So11111111111111111111111111111111111111111' THEN mint_price + tr.amount
            ELSE mint_price
        END AS mint_price,
        pf.mint_currency,
        pf.mint,
        pf.ingested_at
    FROM
        pre_final pf
        LEFT OUTER JOIN transfers tr
        ON tr.tx_id = pf.tx_id
        AND tr.tx_from = pf.purchaser
        AND pf.program_id = 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'
)
SELECT
    *
FROM
    pre_pre_final
WHERE
    program_id <> 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
UNION
SELECT
    block_timestamp,
    block_id,
    tx_id,
    program_id,
    purchaser,
    succeeded,
    SUM(mint_price) over (
        PARTITION BY tx_id,
        purchaser,
        mint,
        mint_currency
    ) AS mint_price,
    mint_currency,
    mint,
    ingested_at
FROM
    pre_pre_final
WHERE
    program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
    AND tx_id NOT IN (
        SELECT
            DISTINCT tx_id
        FROM
            pre_pre_final
        WHERE
            program_id <> 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
    ) qualify(ROW_NUMBER() over (PARTITION BY tx_id, purchaser, mint, mint_currency
ORDER BY
    block_id)) = 1
