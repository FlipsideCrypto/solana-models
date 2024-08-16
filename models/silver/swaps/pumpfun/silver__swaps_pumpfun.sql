 -- depends_on: {{ ref('silver__decoded_logs') }}

{{ config(
    materialized = 'incremental',
    unique_key = "swaps_pumpfun_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(tx_id, swapper, from_mint, to_mint)'
    ),
    tags = ['scheduled_non_core'],
) }}

{% if execute %}
    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.swaps_pumpfun__intermediate_tmp AS
    SELECT
        *
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' 
        and event_type = 'TradeEvent'
        AND succeeded

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '1 hour'
        FROM
            {{ this }}
    )
    {% else %}
        AND _inserted_timestamp :: DATE >= '2024-08-13'
    {% endif %}
    {% endset %}
    
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate(
        "silver.swaps_pumpfun__intermediate_tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

WITH base AS (
    SELECT 
        *
    FROM
        silver.swaps_pumpfun__intermediate_tmp
),
token_decimals AS (
    SELECT 
        mint,
        decimal
    FROM
        solana.silver.decoded_metadata
    UNION ALL 
    SELECT 
        'So11111111111111111111111111111111111111112',
        9
    UNION ALL 
    SELECT 
        'GyD5AvrcZAhSP5rrhXXGPUHri6sbkRpq67xfG3x8ourT',
        9
),


swaps as (
select 
block_timestamp,
block_id,
tx_id,
index,
inner_index,
succeeded,
program_id,
decoded_log:args:isBuy::boolean as is_buy,
decoded_log:args:user::string as swapper,
decoded_log:args:mint::string as mint,
case when is_buy
    then 'So11111111111111111111111111111111111111112'
    else decoded_log:args:mint
    end as from_mint,
case when is_buy
    then decoded_log:args:mint
    else 'So11111111111111111111111111111111111111112'
    end as to_mint,
case when is_buy
    then decoded_log:args:solAmount::string
    else decoded_log:args:tokenAmount::string
    end as from_amount,
case when is_buy
    then decoded_log:args:tokenAmount::string
    else decoded_log:args:solAmount::string
    end as to_amount
    ,
    _inserted_timestamp
from base),

pre_final as (
select
    a.block_timestamp,
    a.block_id,
    a.program_id,
    a.tx_id,
    a.succeeded,
    ROW_NUMBER() over (
        PARTITION BY a.tx_id
        ORDER BY
            a.INDEX,
            a.inner_index
    ) AS swap_index,
    a.index,
    a.inner_index,
    a.swapper,
    a.from_mint,
    a.to_mint,
    a.from_amount as from_amount_int,
    a.from_amount * pow(10,-d.decimal) AS from_amount,
    a.to_amount as to_amount_int,
    a.to_amount * pow(10,-d2.decimal) AS to_amount,
    a._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id','a.index','a.inner_index']) }} as swaps_inner_intermediate_jupiterv6_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from swaps a

LEFT OUTER JOIN
    token_decimals d
    ON a.from_mint = d.mint
LEFT OUTER JOIN
    token_decimals d2
    ON a.to_mint = d2.mint
)
,
distinct_missing_decimals AS (
    SELECT DISTINCT
        to_mint AS mint
    FROM
        pre_final
    WHERE
        to_amount IS NULL
    UNION
    SELECT DISTINCT
        from_mint
    FROM
        pre_final
    WHERE
        from_amount IS NULL
),
get_missing_decimals AS (
    SELECT
        mint,
        solana.live.udf_api(
            'POST',
            '{service}/{Authentication}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            OBJECT_CONSTRUCT(
                'id',
                1,
                'jsonrpc',
                '2.0',
                'method',
                'getAccountInfo',
                'params',
                ARRAY_CONSTRUCT(
                    mint,
                    OBJECT_CONSTRUCT(
                        'encoding',
                        'jsonParsed'
                    )
                )
            ),
            'Vault/prod/solana/quicknode/mainnet'
        ):data:result:value:data:parsed:info:decimals::int AS decimal
    FROM
        distinct_missing_decimals
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    swap_index,
    succeeded,
    program_id,
    swapper,
    from_mint,
    CASE
        WHEN from_amount IS NULL THEN
            from_amount_int * pow(10, -d.decimal)
        ELSE
            from_amount
    END AS from_amount,
    to_mint,
    CASE
        WHEN to_amount IS NULL THEN
            to_amount_int * pow(10, -d2.decimal)
        ELSE
            to_amount
    END AS to_amount,
    _inserted_timestamp,
    swaps_inner_intermediate_jupiterv6_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    pre_final
LEFT OUTER JOIN
    get_missing_decimals d
    ON from_mint = d.mint
LEFT OUTER JOIN
    get_missing_decimals d2
    ON to_mint = d2.mint
