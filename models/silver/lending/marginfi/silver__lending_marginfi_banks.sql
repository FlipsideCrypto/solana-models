-- depends_on: {{ ref('silver__lending_marginfi_deposits') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['lending_marginfi_banks_id'],
    tags = ['scheduled_non_core']
) }}

WITH bank_token_mapping AS (
    SELECT DISTINCT
        bank,
        token_address,
        decimal
    FROM
        {{ ref('silver__lending_marginfi_deposits') }}
    WHERE
        bank IS NOT NULL
        AND token_address IS NOT NULL
            {% if is_incremental() %}
            AND modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
               {% endif %}
)

SELECT 
    bank,
    token_address,
    decimal,
    {{ dbt_utils.generate_surrogate_key(['bank']) }} AS lending_marginfi_banks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    bank_token_mapping
