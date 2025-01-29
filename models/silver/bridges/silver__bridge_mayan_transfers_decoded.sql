-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "bridge_mayan_transfers_decoded_id",
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id,user_address,mint)'),
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.bridge_mayan_transfers_decoded__intermediate_tmp AS
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        signers,
        index,
        inner_index,
        program_id,
        event_type,
        decoded_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'BLZRi6frs4X4DNLw56V4EXai1b6QVESN1BhHBTYM9VcY'
        AND event_type IN ('settle', 'initOrder')
        AND succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2024-06-16'
{% endif %}
{% endset %}

{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.bridge_mayan_transfers_decoded__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.bridge_mayan_transfers_decoded__intermediate_tmp
),

bridge_out AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        succeeded,
        program_id,
        silver.udf_get_account_pubkey_by_name('trader', decoded_instruction:accounts) AS user_address,
        silver.udf_get_account_pubkey_by_name('mintFrom', decoded_instruction:accounts) AS mint,
        decoded_instruction:args:params:amountInMin::int AS mint_amount_raw,
        _inserted_timestamp
    FROM
        base
    WHERE
        event_type = 'initOrder'
),

bridge_out_final AS (
    SELECT
        a.block_timestamp,
        a.block_id,
        a.tx_id,
        a.succeeded,
        a.index,
        a.program_id,
        'mayan finance' AS platform,
        'outbound' AS direction,
        a.user_address,
        (a.mint_amount_raw / POW(10, b.decimal)) AS amount,
        a.mint,
        a._inserted_timestamp
    FROM
        bridge_out a
    LEFT JOIN
        {{ ref('silver__decoded_metadata') }} b
    ON
        a.mint = b.mint
),

bridge_to AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        program_id,
        silver.udf_get_account_pubkey_by_name('dest', decoded_instruction:accounts) AS trader_dest,
        silver.udf_get_account_pubkey_by_name('mintTo', decoded_instruction:accounts) AS mint_to,
        _inserted_timestamp
    FROM
        base
    WHERE
        event_type = 'settle'
),

transfers AS (
    SELECT
        a.block_timestamp,
        a.tx_id,
        a.tx_from,
        a.tx_to,
        a.amount,
        a.mint,
        a.succeeded,
        a.index,
        COALESCE(SPLIT_PART(a.index, '.', 1)::INT, a.index::INT) AS index_1,
        a._inserted_timestamp
    FROM
        {{ ref('silver__transfers') }} a
    INNER JOIN (
        SELECT DISTINCT tx_id
        FROM bridge_to
    ) d ON d.tx_id = a.tx_id
    WHERE
        a.succeeded
        AND {{ between_stmts }}
),

bridge_to_final AS (
    SELECT
        a.block_timestamp,
        a.block_id,
        a.tx_id,
        a.succeeded,
        a.index,
        a.program_id,
        'mayan finance' AS platform,
        'inbound' AS direction,
        a.trader_dest AS user_address,
        b.amount,
        b.mint,
        a._inserted_timestamp
    FROM
        bridge_to a
    LEFT JOIN
        transfers b
    ON
        a.tx_id = b.tx_id
        AND a.trader_dest = b.tx_to
        AND (a.mint_to = b.mint OR a.mint_to = 'So11111111111111111111111111111111111111112')
        AND a.index = b.index_1
    qualify row_number() over(PARTITION BY a.block_id, a.tx_id, a.index ORDER BY a._inserted_timestamp desc) = 1
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index']) }} AS bridge_mayan_transfers_decoded_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bridge_out_final
UNION ALL
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index']) }} AS bridge_mayan_transfers_decoded_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bridge_to_final

