{% macro get_block_production() %}
    {% set create_table %}
    CREATE TABLE if NOT EXISTS {{ target.database }}.bronze_api.block_production(
        json_data VARIANT,
        epoch varchar,
        calls string,
        _inserted_timestamp timestamp_ntz
    );
{% endset %}
    {% do run_query(create_table) %}
    {% set create_block_prod_call_query %}
    CREATE temporary TABLE bronze_api.block_production_call AS
    WITH current_epoch_data AS (
        SELECT
            live.udf_api(
                'POST',
                '{service}/{Authentication}',
                OBJECT_CONSTRUCT(
                    'Content-Type',
                    'application/json'
                ),
                OBJECT_CONSTRUCT(
                    'id',
                    current_timestamp,
                    'jsonrpc',
                    '2.0',
                    'method',
                    'getEpochInfo',
                    'params',
                    []
                ),
                'Vault/prod/solana/quicknode/mainnet'
            ) AS data
    ),
    previous_epoch_start_end AS (
        SELECT
            DATA :data :result :epoch :: INT AS current_epoch,
            current_epoch - 1 AS previous_epoch,
            DATA :data :result :absoluteSlot - DATA :data :result :slotIndex AS current_epoch_start,
            (
                current_epoch_start - 432000
            ) :: INT AS previous_epoch_start,
            (
                current_epoch_start - 1
            ) :: INT AS previous_epoch_end
        FROM
            current_epoch_data
    ),
    blocks_grouped AS (
        SELECT 
            (SELECT max(previous_epoch) FROM previous_epoch_start_end) AS epoch,
            _id,
            floor(_id/5000) AS group_num
        FROM 
            {{ source('crosschain_silver', 'number_sequence') }}
        WHERE _id BETWEEN (SELECT max(previous_epoch_start) FROM previous_epoch_start_end) AND (SELECT max(previous_epoch_end) FROM previous_epoch_start_end)
    ),
    blocks_min_max_per_group AS (
        SELECT 
            epoch,
            group_num,
            min(_id) AS min_block,
            max(_id) AS max_block
        FROM 
            blocks_grouped
        GROUP BY 1,2
    )
    SELECT
        live.udf_api(
            'POST',
            '{service}/{Authentication}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            OBJECT_CONSTRUCT(
                'id',
                concat_ws('-',current_timestamp,group_num),
                'jsonrpc',
                '2.0',
                'method',
                'getBlockProduction',
                'params',
                [{ 
                    'commitment': 
                    'confirmed', 
                    'range': { 
                        'firstSlot': min_block, 
                        'lastSlot': max_block 
                    } 
                }]
            ),
            'Vault/prod/solana/quicknode/mainnet'
        ) AS json_data,
        epoch,
        OBJECT_CONSTRUCT(
            'id',
            concat_ws('-',current_timestamp,group_num),
            'jsonrpc',
            '2.0',
            'method',
            'getBlockProduction',
            'params',
            [{ 
                'commitment': 
                'confirmed', 
                'range': { 
                    'firstSlot': min_block, 
                    'lastSlot': max_block 
                } 
            }]
        )::STRING AS calls,
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS _inserted_timestamp
    FROM
        blocks_min_max_per_group;
    {% endset %}
    {% do run_query(create_block_prod_call_query) %}

    {% set results_query %}
    INSERT INTO bronze_api.block_production (json_data,epoch,calls,_inserted_timestamp)
    SELECT
        json_data,
        epoch,
        calls,
        _inserted_timestamp
    FROM
        bronze_api.block_production_call
    {% endset %}
    {% do run_query(results_query) %}
{% endmacro %}
