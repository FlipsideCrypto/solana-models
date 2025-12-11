
{{ config(
    materialized = 'incremental',
    unique_key = ['stablecoins_transfers_id'],
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
        token_address IS NOT NULL
),

{% if is_incremental() %}

newly_verified_stablecoins AS (
    SELECT
        token_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        IFNULL(
            is_verified_modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ) > DATEADD(
            'day',
            -8,
            (
                SELECT
                    MAX(modified_timestamp) :: DATE
                FROM
                    {{ this }}
            )
        )
),
newly_verified_transfers AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        tx_from,
        tx_to,
        mint,
        amount
    FROM
        {{ ref('silver__transfers') }} a
        INNER JOIN verified_stablecoins b on a.mint = b.token_address
    WHERE
        block_timestamp::date >= (
            SELECT
                MIN(block_date)
            FROM
                {{ ref('silver__stablecoins_daily_supply_by_address') }}
        )
),
{% endif %}



transfers AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        tx_from,
        tx_to,
        mint,
        amount
    FROM
        {{ ref('silver__transfers') }} a
        INNER JOIN verified_stablecoins b on a.mint = b.token_address
    where block_timestamp::date = '2025-12-09'
        
    -- WHERE
    --     block_timestamp::date >= (
    --         SELECT
    --             MIN(block_date)
    --         FROM
    --             solana_dev.silver.stablecoins_daily_supply_by_address
    --     )

)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    tx_from,
    tx_to,
    mint,
    amount,
    {{ dbt_utils.generate_surrogate_key(['tx_id', 'index']) }} AS stablecoins_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    transfers
