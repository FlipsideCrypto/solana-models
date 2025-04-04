-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "nft_sales_tensor_bid_id",
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}
{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE temporary TABLE silver.nft_sales_tensor_bid__intermediate_tmp AS

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        signers,
        INDEX,
        inner_index,
        program_id,
        event_type,
        decoded_instruction,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        (
            (
                program_id = 'TB1Dqt8JeKQh7RLDzfYDJsq8KS4fS2yt87avRjyRxMv' AND event_type = 'takeBid'
            )
            OR (
                program_id = 'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp' AND event_type = 'takeBidLegacy'
            )
        )
        AND succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-03-21'
{% endif %}
{% endset %}

{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.nft_sales_tensor_bid__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        program_id,
        CASE 
            WHEN event_type = 'takeBidLegacy' THEN silver.udf_get_account_pubkey_by_name('owner', decoded_instruction:accounts)
            ELSE silver.udf_get_account_pubkey_by_name('bidder', decoded_instruction:accounts)
        END AS purchaser,
        silver.udf_get_account_pubkey_by_name('seller', decoded_instruction:accounts) AS seller,
        silver.udf_get_account_pubkey_by_name('nftMint', decoded_instruction:accounts) AS mint,
        CASE
            WHEN event_type = 'takeBidLegacy' THEN (decoded_instruction:args:minAmount::int)/pow(10,9)
            ELSE (decoded_instruction:args:lamports::int)/pow(10,9)
        end as sales_amount,
        _inserted_timestamp
    FROM 
        silver.nft_sales_tensor_bid__intermediate_tmp
)

SELECT
    block_timestamp,
    block_id,
    tx_id,
    TRUE as succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','mint']) }} AS nft_sales_tensor_bid_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    decoded