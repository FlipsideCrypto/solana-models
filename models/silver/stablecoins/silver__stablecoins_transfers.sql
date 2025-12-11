
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['stablecoins_transfers_id'],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['daily']
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
        amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__transfers') }} a
        INNER JOIN newly_verified_stablecoins b on a.mint = b.token_address
    WHERE
        block_timestamp::date >= '2025-06-01'
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
        amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__transfers') }} a
        INNER JOIN verified_stablecoins b on a.mint = b.token_address
    WHERE
        block_timestamp::date >= '2025-06-01'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_transfers AS (
    SELECT
        *
    FROM
        transfers

{% if is_incremental()%}
UNION
SELECT
    *
FROM
    newly_verified_transfers
{% endif %}
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
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id', 'index']) }} AS stablecoins_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_transfers
