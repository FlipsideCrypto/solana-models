{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, index, inner_index, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base_burn_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__burn_actions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
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
    A.burn_amount,
    A.burn_authority,
    A.signers,
    b.decimal,
    b.mint_standard_type,
     A._inserted_timestamp
FROM
    base_burn_actions A
    INNER JOIN {{ ref('silver__mint_types') }} b
    ON A.mint = b.mint
WHERE
    b.mint_type = 'token'
