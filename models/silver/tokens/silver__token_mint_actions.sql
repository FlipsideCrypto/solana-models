{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, index, inner_index, mint)",
    incremental_strategy = 'delete+insert',
    incremental_predicates = ['block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id,mint,mint_authority,token_account)'),
    tags = ['scheduled_non_core']
) }}

WITH base_mint_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__mint_actions') }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.succeeded,
    A.index,
    COALESCE(A.inner_index, -1) as inner_index,
    A.event_type,
    A.mint,
    A.mint_amount,
    A.mint_authority,
    A.token_account,
    A.signers,
    b.decimal,
    b.mint_standard_type,
     A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['A.tx_id', 'A.index', 'inner_index', 'A.mint']
    ) }} AS token_mint_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_mint_actions A
    INNER JOIN {{ ref('silver__mint_types') }} b
    ON A.mint = b.mint
WHERE
    b.mint_type = 'token'
