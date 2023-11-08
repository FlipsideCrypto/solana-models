{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    full_refresh = false
) }}

WITH base as (
    SELECT 
        block_id,
        count(*) as transaction_count,
        max(_inserted_timestamp) as _inserted_timestamp
    FROM 
        {{ ref('silver__transactions') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp > (select max(_inserted_timestamp) from {{this}})
    {% else %}
    WHERE 
        block_id >= 226000000
    {% endif %}
    GROUP BY 1
    UNION ALL 
    SELECT 
        block_id,
        count(*),
        max(_inserted_timestamp) as _inserted_timestamp
    FROM 
        {{ ref('silver__votes') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp > (select max(_inserted_timestamp) from {{this}})
    {% else %}
    WHERE 
        block_id >= 226000000
    {% endif %}
    GROUP BY 1
)
SELECT 
    block_id,
    sum(transaction_count) as transaction_count,
    max(_inserted_timestamp) as _inserted_timestamp
FROM 
    base 
GROUP BY 1