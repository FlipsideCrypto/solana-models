
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['stablecoins_mint_burn_id'],
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
        token_address,
        decimals,
        symbol,
        NAME
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

newly_verified_burns AS (
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
        token_burn_actions_id as stablecoins_mint_burn_id
    FROM
        {{ ref('silver__token_burn_actions') }} A
        INNER JOIN newly_verified_stablecoins b
        ON A.mint = b.token_address
    WHERE 
        a.block_timestamp::date >= '2025-06-01'
        and succeeded
),

newly_verified_mints AS (
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
        token_mint_actions_id as stablecoins_mint_burn_id
    FROM
        {{ ref('silver__token_mint_actions') }} A
        INNER JOIN newly_verified_stablecoins b
        ON A.mint = b.token_address
    WHERE 
        a.block_timestamp::date >= '2025-06-01'
        and succeeded
),
{% endif %}

burns AS (
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
        token_burn_actions_id as stablecoins_mint_burn_id
    FROM
        {{ ref('silver__token_burn_actions') }} A
        INNER JOIN verified_stablecoins b
        ON A.mint = b.token_address
    WHERE 
        a.block_timestamp::date >= '2025-06-01'
        and succeeded
    {% if is_incremental() %}
    and modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),

mints AS (
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
        token_mint_actions_id as stablecoins_mint_burn_id
    FROM
        {{ ref('silver__token_mint_actions') }} A
        INNER JOIN verified_stablecoins b
        ON A.mint = b.token_address
    WHERE 
        a.block_timestamp::date >= '2025-06-01'
        and succeeded
    {% if is_incremental() %}
    and modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),

all_mint_burn AS (
    SELECT * FROM burns
    UNION ALL
    SELECT * FROM mints
    
    {% if is_incremental() %}
    UNION ALL
    SELECT * FROM newly_verified_burns
    UNION ALL
    SELECT * FROM newly_verified_mints
    {% endif %}
)

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_name,
    mint,
    amount_raw,
    amount,
    token_account,
    decimal,
    _inserted_timestamp,
    stablecoins_mint_burn_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM all_mint_burn


