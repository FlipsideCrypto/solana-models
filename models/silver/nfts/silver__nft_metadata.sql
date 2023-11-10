{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', contract_address, token_id)",
    incremental_strategy = 'delete+insert',
    tags = ['scheduled_non_core']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ source(
            'legacy_bronze',
            'prod_nft_metadata_uploads_1828572827'
        ) }}
    WHERE
        SPLIT(
            record_content :model :sinks [0] :destination :: STRING,
            '.'
        ) [2] :: STRING = 'nft_metadata'
        AND record_content :model :blockchain :: STRING = 'solana'

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
    (
        b.record_metadata :CreateTime :: INT / 1000
    ) :: TIMESTAMP AS system_created_at,
    record_content :model :blockchain :: STRING AS blockchain,
    t.value :contract_address :: STRING AS contract_address,
    t.value :contract_name :: STRING AS contract_name,
    t.value :created_at_timestamp :: TIMESTAMP AS created_at_timestamp,
    t.value :mint_address :: STRING AS mint,
    t.value :creator_address :: STRING AS creator_address,
    t.value :creator_name :: STRING AS creator_name,
    t.value :image_url :: STRING AS image_url,
    t.value :project_name :: STRING AS project_name,
    t.value :token_id :: STRING AS token_id,
    t.value :token_metadata :: OBJECT AS token_metadata,
    t.value :token_metadata_uri :: STRING AS token_metadata_uri,
    t.value :token_name :: STRING AS token_name,
    b._inserted_timestamp
FROM
    base b,
    LATERAL FLATTEN(
        input => record_content: results
    ) t
WHERE t.value :mint_address :: STRING IS NOT NULL
AND blockchain = 'solana' qualify(ROW_NUMBER() over(PARTITION BY mint
ORDER BY
    created_at_timestamp DESC)) = 1
