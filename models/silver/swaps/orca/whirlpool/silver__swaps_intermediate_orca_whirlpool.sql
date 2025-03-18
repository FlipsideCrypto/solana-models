-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['swaps_intermediate_orca_whirlpool_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core','scheduled_non_core_hourly']
) }}

{% set batch_backfill_size = var('batch_backfill_size', 0) %}
{% set batch_backfill = False if batch_backfill_size == 0 else True %}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.swaps_intermediate_orca_whirlpool__intermediate_tmp AS
        WITH distinct_entities AS (
            SELECT DISTINCT
                tx_id,
                block_timestamp
            FROM 
                {{ ref('silver__decoded_instructions_combined') }}
            WHERE
                program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
                AND succeeded
                AND event_type IN (
                    'swap',
                    'swapV2',
                    'twoHopSwap',
                    'twoHopSwapV2'
                )
                {% if is_incremental() and not batch_backfill %}
                AND _inserted_timestamp >= (
                    SELECT
                        max(_inserted_timestamp) - INTERVAL '1 hour'
                    FROM
                        {{ this }}
                )
                {% elif batch_backfill %}
                    {% set max_block_ts_query %}
                        SELECT max(_inserted_timestamp)::date FROM {{ this }}
                    {% endset %}
                    {% set max_block_ts = run_query(max_block_ts_query)[0][0] %}
                    {% set end_date = max_block_ts + modules.datetime.timedelta(days=batch_backfill_size) %}
                    AND _inserted_timestamp::date BETWEEN '{{ max_block_ts }}' AND '{{ end_date }}'
                {% else %}
                    AND _inserted_timestamp::DATE BETWEEN '2023-11-14' AND '2024-01-01'
                {% endif %}
        )

        SELECT
            d.block_timestamp,
            d.block_id,
            d.tx_id,
            d.signers,
            d.succeeded,
            d.index,
            d.inner_index,
            d.program_id,
            d.event_type,
            d.decoded_instruction,
            d._inserted_timestamp
        FROM
            {{ ref('silver__decoded_instructions_combined') }} d
        JOIN 
            distinct_entities
            USING(tx_id)
        WHERE
            program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
            AND event_type IN (
                'swap',
                'swapV2',
                'twoHopSwap',
                'twoHopSwapV2'
            )
            AND d.block_timestamp >= (
                SELECT
                    min(block_timestamp)
                FROM
                    distinct_entities
            )
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.swaps_intermediate_orca_whirlpool__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.swaps_intermediate_orca_whirlpool__intermediate_tmp
),

decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        succeeded,
        coalesce(lead(inner_index) over (PARTITION BY tx_id, INDEX ORDER BY inner_index) -1, 999999) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name('tokenAuthority', decoded_instruction:accounts) AS swapper,
        -- CASE
        --     WHEN temp_user_source_owner IS NULL THEN 
        --         signers[0]
        --     ELSE temp_user_source_owner
        -- END AS swapper,
        /* don't know if token A or B is the source or destination */
        CASE
            WHEN event_type = 'twoHopSwapV2' THEN
                silver.udf_get_account_pubkey_by_name('tokenOwnerAccountInput', decoded_instruction:accounts)
            ELSE
                silver.udf_get_account_pubkey_by_name('tokenOwnerAccountA', decoded_instruction:accounts)
        END AS token_a_account,
        NULL AS source_mint,
        NULL AS destination_mint,
        CASE
            WHEN event_type = 'twoHopSwapV2' THEN
                silver.udf_get_account_pubkey_by_name('tokenOwnerAccountOutput', decoded_instruction:accounts)
            ELSE
                silver.udf_get_account_pubkey_by_name('tokenOwnerAccountB', decoded_instruction:accounts)
        END AS token_b_account,
        CASE
            WHEN event_type = 'twoHopSwapV2' THEN
                silver.udf_get_account_pubkey_by_name('tokenVaultOneInput', decoded_instruction:accounts)
            ELSE
                silver.udf_get_account_pubkey_by_name('tokenVaultA', decoded_instruction:accounts)
        END AS program_token_a_account,
        CASE
            WHEN event_type = 'twoHopSwapV2' THEN
                silver.udf_get_account_pubkey_by_name('tokenVaultTwoOutput', decoded_instruction:accounts)
            ELSE
                silver.udf_get_account_pubkey_by_name('tokenVaultB', decoded_instruction:accounts)
        END AS program_token_b_account,
        0 AS swap_index,
        _inserted_timestamp
    FROM
        base
    WHERE
        event_type IN ('swap', 'swapV2', 'twoHopSwapV2')
    UNION ALL
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        succeeded,
        coalesce(lead(inner_index) over (PARTITION BY tx_id, INDEX ORDER BY inner_index) -1, 999999) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name('tokenAuthority', decoded_instruction:accounts) AS swapper,
        /* don't know if token A or B is the source or destination */
        silver.udf_get_account_pubkey_by_name('tokenOwnerAccountOneA', decoded_instruction:accounts) AS token_a_account,
        NULL AS source_mint,
        NULL AS destination_mint,
        silver.udf_get_account_pubkey_by_name('tokenOwnerAccountOneB', decoded_instruction:accounts) AS token_b_account,
        silver.udf_get_account_pubkey_by_name('tokenVaultOneA', decoded_instruction:accounts) AS program_token_a_account,
        silver.udf_get_account_pubkey_by_name('tokenVaultOneB', decoded_instruction:accounts) AS program_token_b_account,
        1 AS swap_index,
        _inserted_timestamp
    FROM
        base
    WHERE
        event_type IN ('twoHopSwap')
    UNION ALL
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        succeeded,
        coalesce(lead(inner_index) over (PARTITION BY tx_id, INDEX ORDER BY inner_index) -1, 999999) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name('tokenAuthority', decoded_instruction:accounts) AS swapper,
        /* don't know if token A or B is the source or destination */
        silver.udf_get_account_pubkey_by_name('tokenOwnerAccountTwoA', decoded_instruction:accounts) AS token_a_account,
        NULL AS source_mint,
        NULL AS destination_mint,
        silver.udf_get_account_pubkey_by_name('tokenOwnerAccountTwoB', decoded_instruction:accounts) AS token_b_account,
        silver.udf_get_account_pubkey_by_name('tokenVaultTwoA', decoded_instruction:accounts) AS program_token_a_account,
        silver.udf_get_account_pubkey_by_name('tokenVaultTwoB', decoded_instruction:accounts) AS program_token_b_account,
        2 AS swap_index,
        _inserted_timestamp
    FROM
        base
    WHERE
        event_type IN ('twoHopSwap')
),

transfers AS (
    SELECT
        tr.*,
        coalesce(split_part(index::text, '.', 1)::INT, index::INT) AS index_1,
        nullif(split_part(index::text, '.', 2), '')::INT AS inner_index_1
    FROM
        {{ ref('silver__transfers') }} AS tr
    INNER JOIN (
            SELECT
                DISTINCT tx_id,
                block_timestamp :: DATE AS block_date
            FROM
                decoded
        ) AS d
        ON d.block_date = tr.block_timestamp :: DATE
        AND d.tx_id = tr.tx_id
    WHERE
        tr.succeeded
        AND {{ between_stmts }}
),

swaps_with_amounts AS (
    SELECT
        d.block_id,
        d.block_timestamp,
        d.program_id,
        d.tx_id,
        d.index,
        d.inner_index,
        d.inner_index_end,
        d.succeeded,
        d.swapper,
        coalesce(t1.amount, t2.amount) AS from_amt,
        coalesce(t1.mint, t2.mint) AS from_mint,
        coalesce(t3.amount, t4.amount) AS to_amt,
        coalesce(t3.mint, t4.mint) AS to_mint,
        d.swap_index,
        d._inserted_timestamp
    FROM
        decoded AS d
    LEFT JOIN 
        -- token a is the source
        transfers AS t1
        ON d.tx_id = t1.tx_id
        AND d.token_a_account = t1.source_token_account
        AND d.program_token_a_account = t1.dest_token_account
        AND d.index = t1.index_1
        AND (
            (t1.inner_index_1 BETWEEN d.inner_index AND d.inner_index_end)
            OR d.inner_index IS NULL
        )
    LEFT JOIN 
        -- token b is the source
        transfers AS t2
        ON d.tx_id = t2.tx_id
        AND d.token_b_account = t2.source_token_account
        AND d.program_token_b_account = t2.dest_token_account
        AND d.index = t2.index_1
        AND (
            (t2.inner_index_1 BETWEEN d.inner_index AND d.inner_index_end)
            OR d.inner_index IS NULL
        ) 
    LEFT JOIN 
        -- token a is the destination
        transfers AS t3
        ON d.tx_id = t3.tx_id
        AND d.token_a_account = t3.dest_token_account
        AND d.program_token_a_account = t3.source_token_account
        AND d.index = t3.index_1
        AND (
            (t3.inner_index_1 BETWEEN d.inner_index AND d.inner_index_end)
            OR d.inner_index IS NULL
        )
    LEFT JOIN 
        -- token b is the destination
        transfers AS t4
        ON d.tx_id = t4.tx_id
        AND d.token_b_account = t4.dest_token_account
        AND d.program_token_b_account = t4.source_token_account
        AND d.index = t4.index_1
        AND (
            (t4.inner_index_1 BETWEEN d.inner_index AND d.inner_index_end)
            OR d.inner_index IS NULL
        )  
),

pre_final AS (
    SELECT
        s1.block_id,
        s1.block_timestamp,
        s1.program_id,
        s1.tx_id,
        s1.index,
        s1.inner_index,
        s1.inner_index_end,
        s1.succeeded,
        s1.swapper,
        s1.from_amt,
        s1.from_mint,
        iff(s1.swap_index = 1, s2.to_amt, s1.to_amt) AS to_amt, -- handle two hop swaps
        iff(s1.swap_index = 1, s2.to_mint, s1.to_mint) AS to_mint, -- handle two hop swaps
        s1._inserted_timestamp
    FROM
        swaps_with_amounts AS s1
    LEFT JOIN
        swaps_with_amounts AS s2
        ON s1.tx_id = s2.tx_id
        AND s1.index = s2.index
        AND s1.inner_index IS NOT DISTINCT FROM s2.inner_index
        AND s1.swap_index = 1
        AND s2.swap_index = 2
    WHERE
        s1.swap_index IN (0,1)
    QUALIFY
        row_number() OVER (PARTITION BY s1.tx_id, s1.index, s1.inner_index ORDER BY s1.inner_index) = 1
)

SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    succeeded,
    row_number() OVER (PARTITION BY tx_id ORDER BY index, inner_index) AS swap_index,
    index,
    inner_index,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','swap_index','program_id']) }} AS swaps_intermediate_orca_whirlpool_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
