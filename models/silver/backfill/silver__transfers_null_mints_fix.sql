{{
    config(
        materialized = 'incremental',
        full_refresh = false
    )
}}

{% if execute and is_incremental() %}
    {% set next_block_date_to_process_query %}
        select min(block_timestamp::date)-1 from {{ this }}
    {% endset %}
    {% set next_block_date_to_process = run_query(next_block_date_to_process_query)[0][0] %}
{% endif %}

select 
    t.block_timestamp,
    tr.*,
    (
        silver.udf_get_account_balances_index(dest_token_account, t.account_keys) IS NOT NULL
        OR silver.udf_get_account_balances_index(source_token_account, t.account_keys) IS NOT NULL
    ) AS is_wsol
from solana.silver.transfers_null_mints AS tr
join solana.silver.transactions AS t 
    using(tx_id)
where
    {% if is_incremental() %}
        t.block_timestamp::date = '{{ next_block_date_to_process }}'
    {% else %}
        t.block_timestamp::date = '2024-12-04'
    {% endif %}