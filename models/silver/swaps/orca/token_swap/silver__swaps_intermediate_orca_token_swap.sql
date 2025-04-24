-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['swaps_intermediate_orca_token_swap_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(tx_id, swapper, from_mint, to_mint)'
    ),
    tags = ['scheduled_non_core','scheduled_non_core_hourly']
) }}

{% set batch_backfill_size = var('batch_backfill_size', 0) %}
{% set batch_backfill = False if batch_backfill_size == 0 else True %}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.swaps_intermediate_orca_token_swap__intermediate_tmp AS
        WITH distinct_entities AS (
            SELECT DISTINCT
                tx_id,
                block_timestamp
            FROM 
                {{ ref('silver__decoded_instructions_combined') }}
            WHERE
                program_id in ('9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP', 'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1')
                AND event_type in ('swap')
                AND succeeded
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
                    AND _inserted_timestamp::DATE BETWEEN '2024-12-23' AND '2025-01-01'
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
            program_id in ('9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP', 'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1')
            AND event_type in ('swap')
            AND d.block_timestamp >= (
                SELECT
                    min(block_timestamp)
                FROM
                    distinct_entities
            )
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.swaps_intermediate_orca_token_swap__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.swaps_intermediate_orca_token_swap__intermediate_tmp
),

decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        COALESCE(LEAD(inner_index) OVER (PARTITION BY tx_id, index
            ORDER BY inner_index) -1, 999999
        ) AS inner_index_end,
        program_id,
        silver.udf_get_account_pubkey_by_name('userTransferAuthority', decoded_instruction:accounts) as swapper,
        silver.udf_get_account_pubkey_by_name('source', decoded_instruction:accounts) as source_token_account, 
        null as source_mint,
        null as destination_mint,
        silver.udf_get_account_pubkey_by_name('destination', decoded_instruction:accounts) as destination_token_account,
        silver.udf_get_account_pubkey_by_name('swapDestination', decoded_instruction:accounts) as program_destination_token_account,
        silver.udf_get_account_pubkey_by_name('swapSource', decoded_instruction:accounts) as program_source_token_account,
        _inserted_timestamp
    FROM
        base
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

pre_final AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.program_id,
        A.tx_id,
        A.index,
        A.inner_index,
        A.inner_index_end,
        C.succeeded,
        A.swapper,
        b.amount AS from_amt,
        b.mint AS from_mint,
        C.amount AS to_amt,
        C.mint AS to_mint,
        A._inserted_timestamp
    FROM
        decoded A
        LEFT JOIN transfers b
        ON A.tx_id = b.tx_id
        AND A.source_token_account = b.source_token_account
        AND A.program_source_token_account = b.dest_token_account
        AND A.index = b.index_1
        AND (
            (b.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end)
            OR A.inner_index IS NULL
        )
        LEFT JOIN transfers C
        ON A.tx_id = C.tx_id
        AND A.destination_token_account = C.dest_token_account
        AND A.program_destination_token_account = C.source_token_account
        AND A.index = C.index_1
        AND (
            (C.inner_index_1 BETWEEN A.inner_index AND A.inner_index_end)
            OR A.inner_index IS NULL
        ) 
    QUALIFY ROW_NUMBER() over (PARTITION BY A.tx_id, A.index, A.inner_INDEX ORDER BY inner_index) = 1
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
    {{ dbt_utils.generate_surrogate_key(['tx_id','swap_index','program_id']) }} AS swaps_intermediate_orca_token_swap_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
