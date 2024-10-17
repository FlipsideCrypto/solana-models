  -- depends_on: {{ ref('bronze__streamline_helius_cnft_metadata') }}
  
{{ config(
    materialized = 'incremental',
    unique_key = "mint",
    cluster_by = ['_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(mint,nft_name,nft_collection_id)'),
    tags = ['scheduled_non_core']
) }}

{% if execute and is_incremental() %}
    {% set max_inserted_timestamp_query %} 
    SELECT
        max(_inserted_timestamp)
    FROM
        {{ this }}
    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_timestamp_query)[0][0] %}
{% endif %}

WITH pre_final AS (
    SELECT
        mint,
        data:authorities[0]:address::STRING AS authority,
        data:creators::ARRAY AS creators,
        data:content:metadata:attributes::ARRAY AS metadata,
        data:content:links:image::STRING AS image_url,
        data:content:json_uri::STRING AS metadata_uri,
        data:content:metadata:name::STRING AS nft_name,
        data:grouping[0]:group_key::STRING AS group_key,
        data:grouping[0]:group_value::STRING AS group_value,
        _inserted_timestamp
    FROM
        {% if is_incremental() %}
        {{ ref('bronze__streamline_helius_cnft_metadata') }}
        {% else %}
        {{ ref('bronze__streamline_FR_helius_cnft_metadata') }}
        {% endif %}
    WHERE
        mint IS NOT NULL
        {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
        {% endif %}
    QUALIFY
        row_number() OVER (PARTITION BY mint ORDER BY _inserted_timestamp DESC) = 1
)
SELECT
    mint,
    authority,
    creators,
    metadata,
    image_url,
    metadata_uri,
    nft_name,
    iff(group_key = 'collection', group_value, NULL) AS helius_collection_id,
    iff(helius_collection_id IS NOT NULL, {{ dbt_utils.generate_surrogate_key(['helius_collection_id']) }}, NULL) AS nft_collection_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['mint']) }} AS helius_cnft_metadata_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
