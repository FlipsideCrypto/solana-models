{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id,purchaser,mint)'),
    tags = ['compressed_nft']
) }}

WITH offchain AS (

    SELECT
        r.value :tx_id :: STRING AS tx_id,
        r.value :index :: INTEGER AS mint_index,
        r.value :inner_index :: INTEGER AS mint_inner_index,
        COALESCE(
            r.value :mint :: STRING,
            ''
        ) AS mint,
        0.000005 AS mint_price,
        'So11111111111111111111111111111111111111111' AS mint_currency,
        'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY' AS program_id,
        start_inserted_timestamp AS _inserted_timestamp
    FROM
        {{ ref('bronze_api__parse_compressed_nft_mints') }},
        TABLE(FLATTEN(responses)) AS r
    WHERE
        mint <> '' 
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
    qualify(ROW_NUMBER() over (PARTITION BY tx_id, mint
    ORDER BY
        _inserted_timestamp DESC)) = 1
),
offchain_ordered AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                mint_index,
                mint_inner_index
        ) AS instruction_order
    FROM
        offchain
),
decoded AS (
    SELECT
        decoded_instruction :name :: STRING AS instruction_name,
        *
    FROM
        {{ ref('silver__decoded_instructions') }}
    WHERE
        program_id = 'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY'
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '1 hour'
        FROM
            {{ this }}
    )
    {% endif %}
),
onchain AS (
    SELECT
        m.*,
        d.instruction_name,
        ROW_NUMBER() over (
            PARTITION BY m.tx_id
            ORDER BY
                m.index,
                m.inner_index
        ) AS instruction_order
    FROM
        {{ ref('silver__nft_compressed_mints_onchain') }}
        m
        LEFT OUTER JOIN decoded d
        ON d.tx_id = m.tx_id
        AND d.index = m.index
        AND COALESCE(
            d.inner_index,
            -1
        ) = COALESCE(
            m.inner_index,
            -1
        )
    {% if is_incremental() %}
    WHERE m._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.succeeded,
    b.tx_id,
    A.leaf_owner,
    A.collection_mint,
    b._inserted_timestamp,
    A.creator_address AS purchaser,
    b.mint,
    b.mint_price,
    b.mint_currency,
    b.program_id,
    A.instruction_name,
    {{ dbt_utils.generate_surrogate_key(
        ['b.tx_id','b.mint']
    ) }} AS nft_compressed_mints_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    offchain_ordered b
    LEFT JOIN onchain A
    ON A.tx_id = b.tx_id
    AND A.instruction_order = b.instruction_order
WHERE
    (
        A.instruction_name LIKE 'mint%'
        OR A.instruction_name IS NULL
    )
