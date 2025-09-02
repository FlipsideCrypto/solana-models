-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['lending_kamino_obligations_id'],
    tags = ['scheduled_non_core']
) }}

WITH obligations AS (
    SELECT 
        tx_id,
        block_timestamp,
        case 
            when block_timestamp <= '2024-01-22 19:25:29.000'
            -- the decoded instruction structure changes after above date
            then silver.udf_get_account_pubkey_by_name('feePayer', decoded_instruction:accounts)
            else silver.udf_get_account_pubkey_by_name('obligation', decoded_instruction:accounts)
        end as obligation,
        silver.udf_get_account_pubkey_by_name('obligationOwner', decoded_instruction:accounts) as owner,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD'
        AND event_type = 'initObligation'
            {% if is_incremental() %}
            AND _inserted_timestamp >= (
                SELECT
                    MAX(_inserted_timestamp)
                FROM
                    {{ this }}
            )
               {% endif %}
)

SELECT 
    tx_id,
    block_timestamp,
    obligation,
    owner,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['obligation']) }} AS lending_kamino_obligations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    obligations
QUALIFY
    row_number() OVER (PARTITION BY obligation ORDER BY block_timestamp) = 1
