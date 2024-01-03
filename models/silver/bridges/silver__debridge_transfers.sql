{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['debridge_api']
) }}
-- WITH base AS (

SELECT
    tx_id,
    json_data [0] :giveOffer :amount :stringValue :: INT AS amount_sent,
    json_data [0] :giveOffer :chainId :stringValue :: INT AS origin_chain,
    a.chain_name as origin_chain_name,
    json_data [0] :giveOffer :tokenAddress :stringValue :: STRING AS token_sent,
    json_data [0] :makerSrc :stringValue :: STRING AS address_sending,
    json_data [0] :orderId :stringValue :: STRING AS debridge_order_id,
    json_data [0] :receiverDst :stringValue :: STRING AS recieving_address,
    json_data [0] :takeOffer :amount :stringValue :: INT AS amount_recieved,
    json_data [0] :takeOffer :chainId :stringValue :: INT AS destination_chain,
    b.chain_name as destination_chain_name,
    json_data [0] :takeOffer :tokenAddress :stringValue :: STRING AS token_recieved,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__debridge_txs') }}
    LEFT JOIN {{ ref('seed__debridge_chain_id') }} a
    ON origin_chain :: STRING = a.debridge_chain_id :: STRING
    LEFT JOIN {{ ref('seed__debridge_chain_id') }} b
    ON destination_chain :: STRING = a.debridge_chain_id :: STRING
    -- {% if is_incremental() %}
    -- and
    --     _inserted_timestamp >= (
    --         SELECT
    --             MAX(_inserted_timestamp)
    --         FROM
    --             {{ this }}
    --     )
    -- {% endif %}
    -- )
