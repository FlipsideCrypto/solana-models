{{
    config(
        materialized="incremental",
        unique_key="mint",
        cluster_by=["_inserted_timestamp::date"]
    )
}}

SELECT
    mint,
    helius_nft_metadata_requests_id,
    max_mint_event_inserted_timestamp,
    _partition_id,
    _inserted_timestamp
FROM
{% if is_incremental() %}
    {{ ref('bronze__streamline_helius_nft_metadata') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_helius_nft_metadata') }}
{% endif %}
QUALIFY
    row_number() OVER (PARTITION BY mint ORDER BY _inserted_timestamp DESC) = 1