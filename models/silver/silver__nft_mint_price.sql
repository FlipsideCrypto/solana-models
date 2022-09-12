{{ config(
    materialized = 'view'
) }}

with base as (
    select 
        block_timestamp,
        tx_ids,
        program_id,
        payer,
        mint,
        mint_currency,
        mint_price,
        _inserted_timestamp,
        1 as ranking
    from 
        {{ ref('silver__nft_mint_price_candy_machine') }}
    union 
    select 
        block_timestamp,
        tx_ids,
        program_id,
        payer,
        mint,
        mint_currency,
        mint_price,
        _inserted_timestamp,
        2 as ranking
    from 
        {{ ref('silver__nft_mint_price_other') }}
    where 
        mint_price is not null
    union 
    select 
        block_timestamp,
        tx_ids,
        program_id,
        payer,
        mint,
        mint_currency,
        mint_price,
        _inserted_timestamp,
        99 as ranking
    from 
        {{ ref('silver__nft_mint_price_generic') }}
    where 
        mint_price is not null
)
select 
    *
from base 
qualify(row_number() over (partition by mint, mint_currency order by ranking)) = 1