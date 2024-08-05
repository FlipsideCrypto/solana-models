-- depends_on: {{ ref('silver__events') }}
{{ config(
    materialized = 'incremental',
    unique_key = "liquidity_pool_actions_meteora_multi_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core'],
) }}

{% if execute %}
    {% set base_query %}
    CREATE
    OR REPLACE temporary TABLE silver.liquidity_pool_actions_meteora_multi__intermediate_tmp AS

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        program_id,
        instruction,
        signers,
        instruction :accounts [0] :: STRING AS liquidity_pool_address,
        signers [0] :: STRING AS liquidity_provider,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id = 'MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky'
        AND succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2021-06-23'
{% endif %}

{% endset %}
{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate("silver.liquidity_pool_actions_meteora_multi__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.liquidity_pool_actions_meteora_multi__intermediate_tmp
),
base_transfers AS (
    SELECT
        block_timestamp,
        tx_id,
        INDEX,
        mint,
        amount,
        dest_token_account,
        source_token_account,
        tx_to,
        tx_from
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        {{ between_stmts }}
),
base_mint_actions AS (
    SELECT
        block_timestamp,
        tx_id,
        INDEX,
        inner_index,
        event_type,
        mint,
        mint_amount,
        mint_authority,
        DECIMAL
    FROM
        {{ ref('silver__token_mint_actions') }}
    WHERE
        {{ between_stmts }}
),
base_burn_actions AS (
    SELECT
        block_timestamp,
        tx_id,
        INDEX,
        inner_index,
        event_type,
        mint,
        burn_amount,
        burn_authority,
        DECIMAL
    FROM
        {{ ref('silver__token_burn_actions') }}
    WHERE
        {{ between_stmts }}
),
mints AS (
    SELECT
        d.block_timestamp,
        d.block_id,
        d.tx_id,
        d.succeeded,
        m.index,
        m.inner_index,
        d.program_id,
        m.event_type AS action,
        m.mint,
        m.mint_amount * pow(
            10,- m.decimal
        ) AS amount,
        d.liquidity_provider,
        d.liquidity_pool_address,
        d._inserted_timestamp
    FROM
        base_mint_actions m
        INNER JOIN base d
        ON m.block_timestamp :: DATE = d.block_timestamp :: DATE
        AND m.tx_id = d.tx_id
        AND m.index = d.index
    WHERE
        m.mint_authority = d.instruction :accounts [2]
),
burns AS (
    SELECT
        d.block_timestamp,
        d.block_id,
        d.tx_id,
        d.succeeded,
        m.index,
        m.inner_index,
        d.program_id,
        m.event_type AS action,
        m.mint,
        m.burn_amount * pow(
            10,- m.decimal
        ) AS amount,
        d.liquidity_provider,
        d.liquidity_pool_address,
        d._inserted_timestamp
    FROM
        base_burn_actions m
        INNER JOIN base d
        ON m.block_timestamp :: DATE = d.block_timestamp :: DATE
        AND m.tx_id = d.tx_id
        AND m.index = d.index
    WHERE
        m.burn_authority = d.instruction :accounts [3]
),
deposits AS (
    SELECT
        b.block_timestamp,
        b.block_id,
        b.tx_id,
        b.succeeded,
        b.index,
        SPLIT_PART(
            t.index,
            '.',
            2
        ) :: INT AS inner_index,
        b.program_id,
        'deposit' AS action,
        t.mint,
        t.amount,
        b.liquidity_provider,
        b.liquidity_pool_address,
        b._inserted_timestamp
    FROM
        base_transfers t
        INNER JOIN base b
        ON t.block_timestamp :: DATE = b.block_timestamp :: DATE
        AND t.tx_id = b.tx_id
        AND SPLIT_PART(
            t.index,
            '.',
            1
        ) = b.index
        INNER JOIN mints C
        ON t.tx_id = C.tx_id
        AND SPLIT_PART(
            t.index,
            '.',
            1
        ) = C.index
    WHERE
        t.tx_from = b.signers [0]
        OR t.tx_from = b.instruction :accounts [3]
),
withdraws AS (
    SELECT
        b.block_timestamp,
        b.block_id,
        b.tx_id,
        b.succeeded,
        b.index,
        SPLIT_PART(
            t.index,
            '.',
            2
        ) :: INT AS inner_index,
        b.program_id,
        'withdraw' AS action,
        t.mint,
        t.amount,
        b.liquidity_provider,
        b.liquidity_pool_address,
        b._inserted_timestamp
    FROM
        base_transfers t
        INNER JOIN base b
        ON t.block_timestamp :: DATE = b.block_timestamp :: DATE
        AND t.tx_id = b.tx_id
        AND SPLIT_PART(
            t.index,
            '.',
            1
        ) = b.index
        INNER JOIN burns C
        ON t.tx_id = C.tx_id
        AND SPLIT_PART(
            t.index,
            '.',
            1
        ) = C.index
    WHERE
        t.tx_from = b.instruction :accounts [2]
),
pre_final AS (
    SELECT
        *
    FROM
        deposits
    UNION ALL
    SELECT
        *
    FROM
        withdraws
    UNION ALL
    SELECT
        *
    FROM
        mints
    UNION ALL
    SELECT
        *
    FROM
        burns
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    program_id,
    action,
    mint,
    amount,
    liquidity_provider,
    liquidity_pool_address,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index']) }} AS liquidity_pool_actions_meteora_multi_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
