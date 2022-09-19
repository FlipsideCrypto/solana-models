{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', mint, payer, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

WITH base_candy_machine_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE 
        program_id in ('cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ','cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ')
    AND 
        succeeded

{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
base_ptb AS (
    SELECT
        DISTINCT mint AS mint_paid,
        account,
        DECIMAL
    FROM
        {{ ref('silver___post_token_balances') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
candy_machine AS (
    SELECT
        e.block_timestamp,
        e.tx_id,
        e.index,
        i.index as inner_index,
        e.program_id,
        instruction :accounts [5] :: STRING AS mint,
        instruction :accounts [2] :: STRING AS payer,
        COALESCE(
            i.value :parsed :info :lamports :: INTEGER,
            i.value :parsed :info :amount :: INTEGER
        ) AS transfer_amount,
        CASE 
            WHEN i.value :parsed :info :lamports is not null THEN NULL 
            ELSE i.value :parsed :info :source
        END as token_account,
        e._inserted_timestamp
    FROM
        base_candy_machine_events e
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
    group by 1,2,3,4,5,6,7,8,9,10
),
pre_final as (
    select 
        e.*,
        COALESCE(
            p.mint_paid,
            'So11111111111111111111111111111111111111111'
        ) AS mint_currency,
        COALESCE(p.decimal, 9) as decimal
    from candy_machine e
    LEFT OUTER JOIN base_ptb p on e.token_account = p.account
)
SELECT
    p.mint,
    p.payer,
    mint_currency,
    decimal,
    p.program_id,
    SUM(p.transfer_amount / pow(10, decimal)) AS mint_price,
    array_unique_agg(tx_id) as tx_ids,
    max(block_timestamp) as block_timestamp,
    max(_inserted_timestamp) as _inserted_timestamp
FROM
    pre_final p
WHERE 
    p.mint not in ('Sysvar1nstructions1111111111111111111111111','SysvarRent111111111111111111111111111111111') -- not mint events
GROUP BY 
    1, 
    2, 
    3, 
    4, 
    5