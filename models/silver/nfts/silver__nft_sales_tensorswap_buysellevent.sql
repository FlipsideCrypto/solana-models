-- depends_on: {{ ref('silver__decoded_logs') }}

{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "nft_sales_tensorswap_buysellevent_id",
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE temporary TABLE silver.nft_sales_tensorswap_buysellevent__intermediate_tmp AS
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        TRUE AS succeeded,
        INDEX,
        inner_index,
        log_index,
        program_id,
        event_type,
        decoded_log,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        program_id = 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
        AND event_type = 'BuySellEvent'
    {% if is_incremental() %}
        AND _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) - INTERVAL '1 hour'
            FROM
                {{ this }}
        )
    {% else %}
        AND _inserted_timestamp :: DATE >= '2024-06-02'
    {% endif %}
{% endset %}

{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.nft_sales_tensorswap_buysellevent__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX,
        inner_index,
        log_index,
        program_id,
        event_type,
        decoded_log:args:creatorsFee::int AS creator_fee,
        decoded_log:args:currentPrice::int AS current_price,
        decoded_log:args:mmFee::int AS mm_fee,
        decoded_log:args:tswapFee::int AS tswap_fee,
        _inserted_timestamp
    FROM
        silver.nft_sales_tensorswap_buysellevent__intermediate_tmp
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    log_index,
    program_id,
    event_type,
    creator_fee,
    mm_fee,
    tswap_fee,
    current_price AS sales_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index','log_index']) }} AS nft_sales_tensorswap_buysellevent_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    decoded