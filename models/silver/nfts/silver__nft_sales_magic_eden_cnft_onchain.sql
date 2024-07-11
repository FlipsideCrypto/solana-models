{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    unique_key = ['nft_sales_magic_eden_cnft_onchain_id'],
    cluster_by = ['block_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}

-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set max_inserted_query %}

SELECT
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query).columns [0].values() [0] %}
{% endif %}

{% set base_query %}
CREATE OR REPLACE temporary TABLE silver.decoded_instructions_magic_eden__intermediate_tmp AS
SELECT
    block_timestamp,
    block_id,
    tx_id,
    INDEX,
    inner_index,
    program_id,
    decoded_instruction,
    event_type,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_instructions_combined') }}
WHERE
    program_id = 'M3mxk5W2tt27WGT7THox7PmgRDp4m6NEhL5xvxrBfS1'
    AND event_type = 'buyNow'

{% if is_incremental() %}
AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% else %}
    AND block_timestamp :: DATE >= '2023-08-17'
{% endif %}

{% endset %}
{% do run_query(
    base_query
) %}

{% set between_stmts = fsc_utils.dynamic_range_predicate("silver.decoded_instructions_magic_eden__intermediate_tmp","block_timestamp::date") %}
{% endif %}

with decoded AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.index,
        A.inner_index,
        A.program_id,
        silver.udf_get_account_pubkey_by_name('buyer',A.decoded_instruction :accounts) AS purchaser,
        silver.udf_get_account_pubkey_by_name('seller',A.decoded_instruction :accounts) AS seller,
        silver.udf_get_account_pubkey_by_name('treeAuthority',a.decoded_instruction :accounts) AS tree_authority,
        silver.udf_get_account_pubkey_by_name('merkleTree',a.decoded_instruction :accounts) AS merkle_tree,
        decoded_instruction:args:args:index::int as leaf_index,
        _inserted_timestamp
    FROM
        silver.decoded_instructions_magic_eden__intermediate_tmp A
),
transfers AS (
    SELECT
        A.block_timestamp,
        A.tx_id,
        A.tx_from,
        A.succeeded,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 1) :: INT, INDEX :: INT) AS index_1,
        SUM(A.amount) as sales_amount
    FROM
        {{ ref('silver__transfers') }} A
        INNER JOIN (
            SELECT
                tx_id,
                block_timestamp::date as dt
            FROM
                decoded
        ) d
        ON d.dt = a.block_timestamp::DATE
            AND d.tx_id = A.tx_id
    WHERE
        A.succeeded
        and {{ between_stmts }}
    group by 1,2,3,4,5
),
pre_final as (
SELECT
    A.block_id,
    A.block_timestamp,
    A.program_id,
    A.tx_id,
    b.succeeded,
    A.purchaser,
    A.seller,
    a.tree_authority,
    a.merkle_tree,
    a.leaf_index,
    A._inserted_timestamp,
    b.sales_amount
FROM
    decoded A
    LEFT JOIN transfers b
    ON A.tx_id = b.tx_id
    AND A.purchaser = b.tx_from
    AND A.index = b.index_1
)

SELECT 
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    succeeded,
    purchaser,
    seller,
    tree_authority,
    merkle_tree,
    leaf_index,
    _inserted_timestamp,
    sales_amount,
  {{ dbt_utils.generate_surrogate_key(['tx_id','leaf_index','purchaser']) }} as nft_sales_magic_eden_cnft_onchain_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final