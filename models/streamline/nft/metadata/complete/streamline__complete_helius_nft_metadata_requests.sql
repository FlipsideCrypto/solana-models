-- depends_on: {{ ref('bronze__streamline_helius_nft_metadata') }}

{{
    config(
        materialized = 'incremental',
        unique_key = 'mint',
        cluster_by = ['_inserted_timestamp::date']
    )
}}

SELECT
    mint,
    helius_nft_metadata_requests_id,
    max_mint_event_inserted_timestamp,
    _partition_by_created_date,
    _inserted_timestamp
FROM
    {% if is_incremental() %}
    {{ ref('bronze__streamline_helius_nft_metadata') }}
    {% else %}
    {{ ref('bronze__streamline_FR_helius_nft_metadata') }}
    {% endif %}
{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            coalesce(max(_inserted_timestamp), '1970-01-01'::DATE) max_inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
QUALIFY
    row_number() OVER (
        PARTITION BY mint
        ORDER BY _inserted_timestamp DESC
    ) = 1