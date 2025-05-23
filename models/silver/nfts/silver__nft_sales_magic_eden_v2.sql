{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core'],
    full_refresh = false,
    enabled = false
) }}

/* run incremental timestamp value first then use it as a static value */
{% if execute %}
    {% set create_tmp_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.nft_sales_magic_eden_v2__intermediate_tmp AS
        SELECT
            block_timestamp,
            tx_id,
            succeeded,
            signers[0] :: STRING AS signer, 
            MAX(INDEX) AS max_event_index
        FROM
            {{ ref('silver__events') }}
        WHERE
            program_id = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' -- Magic Eden V2 Program ID
            {% if is_incremental() %}
            {{ get_batch_load_logic(this, 30, '2024-07-12') }}
            {% else %}
            AND _inserted_timestamp::date BETWEEN '2022-08-12' AND '2022-09-01'
            {% endif %}
        GROUP BY
            1,2,3,4
        HAVING count(tx_id) >= 2
    {% endset %}
    {% do run_query(create_tmp_query) %}

    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.nft_sales_magic_eden_v2__intermediate_tmp","block_timestamp::date","e") %}
{% endif %}

WITH txs AS (
    SELECT
        block_timestamp::date AS block_date,
        tx_id,
        succeeded,
        signer, 
        max_event_index
    FROM
        silver.nft_sales_magic_eden_v2__intermediate_tmp
),
base_tmp AS (
    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        t.succeeded,
        e.index AS event_index,
        i.index AS inner_index,
        e.program_id,
        COALESCE(
            i.value :parsed :info :lamports :: NUMBER,
            0
        ) AS amount,
        instruction :accounts [7] :: STRING AS nft_account,
        instruction :accounts [0] :: STRING AS purchaser,
        i.value :parsed :type :: STRING AS inner_instruction_type,
        LAG(inner_instruction_type) over (
            PARTITION BY e.tx_id
            ORDER BY
                inner_index
        ) AS preceding_inner_instruction_type,
        -- some mints do not map to a token account because of post purchase transfers within same transaction...need to use this when it is available
        LAST_VALUE(
            i.value :parsed :info :mint :: STRING ignore nulls
        ) over (
            PARTITION BY e.tx_id
            ORDER BY
                inner_index
        ) AS nft_account_mint,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN txs t
        ON t.block_date = e.block_timestamp::date
        AND t.tx_id = e.tx_id
        AND t.max_event_index = e.index
        AND ARRAY_SIZE(
            e.inner_instruction :instructions
        ) > 1
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE
        (
            (
                amount <> 0
                AND inner_instruction_type = 'transfer'
            )
            OR inner_instruction_type = 'create'
        )
        AND array_size(e.instruction:accounts) > 12
        AND {{ between_stmts }}
        {% if is_incremental() %}
            {% if execute %}
            {{ get_batch_load_logic(this, 30, '2024-07-04') }}
            {% endif %}
        {% else %}
        AND _inserted_timestamp::date BETWEEN '2022-08-12' AND '2022-09-01'
        {% endif %}
),
sellers AS (
     SELECT
        e.tx_id,
        CASE
            WHEN instruction :accounts [0] = instruction :accounts [1]
            THEN instruction :accounts [2] :: STRING
            ELSE instruction :accounts [1] :: STRING
        end as seller
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN txs t
        ON t.block_date = e.block_timestamp::date
        AND t.tx_id = e.tx_id
        AND t.max_event_index = e.index
        AND ARRAY_SIZE(
            e.inner_instruction :instructions
        ) > 1
        LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    WHERE 
        (
            (
                i.value :program :: STRING = 'spl-token'
                AND i.value :programId :: STRING = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                AND i.value :parsed :type :: STRING in ('transfer','closeAccount')
            )
            OR 
            (
                i.value :program :: STRING = 'system'
                AND i.value :programId :: STRING = '11111111111111111111111111111111'
                AND i.value :parsed :type :: STRING in ('transfer')
            )
        )
        AND {{ between_stmts }}
        {% if is_incremental() %}
            {% if execute %}
            {{ get_batch_load_logic(this, 30, '2024-07-04') }}
            {% endif %}
        {% else %}
        AND _inserted_timestamp::date BETWEEN '2022-08-12' AND '2022-09-01'
        {% endif %}
    qualify(row_number() over (partition by e.tx_id order by i.index desc)) = 1
),
base AS (
    SELECT
        *
    FROM
        base_tmp
    WHERE
        inner_instruction_type = 'transfer'
),
post_token_balances AS (
    SELECT
        DISTINCT tx_id,
        account,
        mint
    FROM
        {{ ref('silver___post_token_balances') }}
    WHERE
        1=1
        {% if is_incremental() %}
            {% if execute %}
            {{ get_batch_load_logic(this, 30, '2024-07-04') }}
            {% endif %}
        {% else %}
        AND _inserted_timestamp::date BETWEEN '2022-08-12' AND '2022-09-01'
        {% endif %}
)
SELECT
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.succeeded,
    b.event_index as index,
    null as inner_index,
    b.program_id,
    COALESCE(
        b.nft_account_mint,
        p.mint, 
        b.nft_account
    ) AS mint,
    b.purchaser,
    ss.seller, 
    SUM(
        b.amount
    ) / pow(
        10,
        9
    ) AS sales_amount,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['b.tx_id']
    ) }} AS nft_sales_magic_eden_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base b
    LEFT OUTER JOIN post_token_balances p
    ON p.tx_id = b.tx_id
    AND p.account = b.nft_account
    LEFT OUTER JOIN sellers ss
    ON ss.tx_id = b.tx_id
GROUP BY
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.succeeded,
    b.event_index,
    b.program_id,
    COALESCE(
        b.nft_account_mint,
        p.mint, 
        b.nft_account
    ),
    b.purchaser,
    ss.seller, 
    b._inserted_timestamp
