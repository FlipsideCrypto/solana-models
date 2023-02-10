{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","tx_id"],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

base_mint_actions AS (
    SELECT
        m.*,
        e.signers [0] :: STRING AS liquidity_provider
    FROM
        solana.silver.mint_actions
        m
        left outer join base_events e
        ON e.tx_id = m.tx_id
        and e.index = m.index
    WHERE
        m.event_type = 'mintTo'
    
    where block_timestamp::date = '2022-04-14'

)