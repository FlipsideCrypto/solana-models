{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

WITH base_events AS(

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id IN (
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h',
            '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
        )
-- {% if is_incremental() %}
-- AND _inserted_timestamp >= (
--     SELECT
--         MAX(_inserted_timestamp)
--     FROM
--         {{ this }}
-- )
-- {% else %}
--     AND block_timestamp :: DATE >= '2021-03-06'
-- {% endif %}
)
select
 block_timestamp,
 block_id,
 tx_id,
instruction:accounts[3]::string as liquidity_pool,
instruction:accounts[16]::string as owner,
instruction:accounts[4]::string as mint_authority,
instruction:accounts[9]::string  as token_a_account,
instruction:accounts[10]::string  as token_b_account,
instruction:accounts[6]::string as pool_token,
 _inserted_timestamp
from base_events e
where program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
and event_type is null
and succeeded
and instruction:accounts[0] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
and instruction:accounts[1] = '11111111111111111111111111111111'
and instruction:accounts[2] = 'SysvarRent111111111111111111111111111111111'
and instruction:accounts[4] = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1'

union

select
 block_timestamp,
 block_id,
 tx_id,
instruction:accounts[3]::string as liquidity_pool,
'5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h' as owner,
instruction:accounts[4]::string as mint_authority,
instruction:accounts[9]::string  as token_a_account,
instruction:accounts[10]::string  as token_b_account,
instruction:accounts[6]::string as pool_token,
 _inserted_timestamp
from base_events e
where program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
and event_type is null
and succeeded
and instruction:accounts[0] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
and instruction:accounts[1] = '11111111111111111111111111111111'
and instruction:accounts[2] = 'SysvarRent111111111111111111111111111111111'

union

select
 block_timestamp,
 block_id,
 tx_id,
instruction:accounts[1]::string as liquidity_pool,
'27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv' as owner,
instruction:accounts[2]::string as mint_authority,
instruction:accounts[7]::string  as token_a_account,
instruction:accounts[8]::string  as token_b_account,
instruction:accounts[4]::string as pool_token,
 _inserted_timestamp
from base_events e
where program_id = '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv'
and event_type is null
and succeeded
and instruction:accounts[0] = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
and instruction:accounts[13] = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'