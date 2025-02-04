{{ config(
    materialized = 'incremental',
    unique_key = ['nft_sales_solsniper_cnft_id'],
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH mint_addresses AS (

    SELECT
        r.value :tx_id :: STRING AS tx_id,
        COALESCE(
            r.value :mint :: STRING,
            ''
        ) AS mint,
        r.value :index :: INTEGER AS mint_index,
        r.value :inner_index :: INTEGER AS mint_inner_index,
        _inserted_timestamp AS mint_inserted_timestamp
    FROM
        {{ ref('bronze_api__parse_compressed_nft_sales_solsniper') }},
        TABLE(FLATTEN(responses)) AS r
    WHERE
        mint <> ''
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(mint_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
    qualify(ROW_NUMBER() over (PARTITION BY tx_id, mint
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.program_id,
    A.tx_id,
    A.succeeded,
    A.purchaser,
    A.seller,
    A.tree_authority,
    A.merkle_tree,
    A.leaf_index,
    b.mint,
    b.mint_inserted_timestamp,
    A.sales_amount,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','b.mint']
    ) }} AS nft_sales_solsniper_cnft_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__nft_sales_solsniper_cnft_onchain') }} A
    INNER JOIN mint_addresses b
    ON A.tx_id = b.tx_id

{% if is_incremental() %}
WHERE A._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '2 hour'
    FROM
        {{ this }}
)
{% endif %}
