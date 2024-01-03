{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['wormhole_api']
) }}

WITH base AS (

    SELECT
        items.value ['txHash'] :: STRING AS tx_id,
        items.value ['timestamp'] AS tx_timestamp,
        'wormhole' AS platform,
        items.value ['emitterAddress'] :: STRING AS emitter_address,
        items.value ['emitterChain'] :: INT AS emitter_chain,
        items.value ['emitterNativeAddress'] :: STRING AS from_address,
        items.value ['id'] :: STRING AS id,
        items.value ['payload'] AS payload,
        -- IDK IF PAYLOAD Should be parsed, if info is relevant i think it just intermediary idk
        items.value ['payload'] ['amount'] :: INT AS amount_raw,
        -- items.value ['payload']['fromAddress'] as fromAddress_p,
        -- items.value ['payload']['parsedPayload'] as parsedPayload_p,
        -- items.value ['payload']['payload'] as payload_p,
        items.value ['payload'] ['toAddress'] AS to_address_p,
        items.value ['standardizedProperties'] AS standardized_properties,
        -- case when items.value ['standardizedProperties']['amount'] = ''
        --     then null
        --     else items.value ['standardizedProperties']['amount']::int
        --     end as amount, -- more nulls then other amount -- plus decimal seems off
        CASE
            WHEN items.value ['standardizedProperties'] ['fee'] = '' THEN NULL
            ELSE items.value ['standardizedProperties'] ['fee'] :: INT
        END AS fee,
        CASE
            WHEN items.value ['standardizedProperties'] ['feeAddress'] = '' THEN NULL
            ELSE items.value ['standardizedProperties'] ['feeAddress'] :: STRING
        END AS fee_address,
        items.value ['standardizedProperties'] ['feeChain'] :: INT AS fee_chain,
        CASE
            WHEN items.value ['standardizedProperties'] ['fromAddress'] = '' THEN NULL
            ELSE items.value ['standardizedProperties'] ['fromAddress'] :: STRING
        END AS idk_from_address_wormhole,
        items.value ['standardizedProperties'] ['fromChain'] :: INT AS from_chain,
        CASE
            WHEN items.value ['standardizedProperties'] ['toAddress'] = '' THEN NULL
            ELSE items.value ['standardizedProperties'] ['toAddress'] :: STRING
        END AS to_address,
        CASE
            WHEN items.value ['standardizedProperties'] ['appIds'] = 'null' THEN NULL
            ELSE items.value ['standardizedProperties'] ['appIds']
        END AS app_ids,
        items.value ['standardizedProperties'] ['toChain'] :: INT AS to_chain,
        items.value ['standardizedProperties'] ['tokenAddress'] :: STRING AS token_address,
        items.value ['standardizedProperties'] ['tokenChain'] :: INT AS token_chain,
 
        _inserted_timestamp
    FROM
        {{ source(
            'bronze_api',
            'wormhole_txs'
        ) }},
        LATERAL FLATTEN(
            input => json_data :data :transactions
        ) AS items

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    base.*,
    A.chain_name AS emitter_chain_name,
    b.chain_name AS fee_chain_name,
    C.chain_name AS from_chain_name,
    d.chain_name AS to_chain_name,
    {{ dbt_utils.generate_surrogate_key(
        ['base.tx_id']
    ) }} AS wormhole_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
    LEFT JOIN {{ ref('seed__wormhole_chain_id') }} A
    ON emitter_chain :: STRING = A.wormhole_chain_id :: STRING
    LEFT JOIN {{ ref('seed__wormhole_chain_id') }} b
    ON fee_chain :: STRING = b.wormhole_chain_id :: STRING
    LEFT JOIN {{ ref('seed__wormhole_chain_id') }} C
    ON from_chain :: STRING = C.wormhole_chain_id :: STRING
    LEFT JOIN {{ ref('seed__wormhole_chain_id') }} d
    ON to_chain :: STRING = d.wormhole_chain_id :: STRING
