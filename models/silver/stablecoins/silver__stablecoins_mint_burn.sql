
{{ config(
    materialized = 'incremental',
    unique_key = ['stablecoins_mint_burn_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH verified_stablecoins AS (

    SELECT
        token_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        and token_address IS NOT NULL
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    'Burn' as event_name,
    mint,
    burn_amount as amount_raw,
    burn_amount / pow(10,decimal) as amount,
    token_account,
    decimal,
     _inserted_timestamp,
     token_burn_actions_id as stablecoins_mint_burn_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__token_burn_actions') }} A
        INNER JOIN verified_stablecoins b
        ON A.mint = b.token_address
    where a.block_timestamp::date = '2025-12-09'
    and succeeded
-- {% if is_incremental() %}
-- WHERE
--     _inserted_timestamp >= (
--         SELECT
--             MAX(_inserted_timestamp)
--         FROM
--             {{ this }}
--     )

{% endif %}
union all
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    'Mint' as event_name,
    mint,
    mint_amount as amount_raw,
    mint_amount / pow(10,decimal) as amount,
    token_account,
    decimal,
     _inserted_timestamp,
     token_mint_actions_id as stablecoins_mint_burn_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__token_mint_actions') }} A
        INNER JOIN verified_stablecoins b
        ON A.mint = b.token_address
    where a.block_timestamp::date = '2025-12-09'
    and succeeded
-- {% if is_incremental() %}
-- WHERE
--     _inserted_timestamp >= (
--         SELECT
--             MAX(_inserted_timestamp)
--         FROM
--             {{ this }}
--     )
-- {% endif %}


