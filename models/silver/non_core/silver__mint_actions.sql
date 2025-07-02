{{ config(
    materialized = 'incremental',
    unique_key = ['mint_actions_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = false,
    tags = ['scheduled_non_core','scheduled_non_core_hourly']
) }}

{% if execute %}

    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(_inserted_timestamp) AS max_inserted_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_inserted_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

WITH prefinal as (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        index,
        null as inner_index,
        event_type,
        instruction :parsed :info :mint :: STRING AS mint,
        instruction :parsed :info :account :: STRING as token_account, 
        instruction :parsed :info :decimals :: INTEGER AS DECIMAL,
        COALESCE(
            instruction :parsed :info :amount :: INTEGER,
            instruction :parsed :info :tokenAmount: amount :: INTEGER
        ) AS mint_amount,
        COALESCE(
            instruction :parsed :info :mintAuthority :: string,
            instruction :parsed :info :multisigMintAuthority :: string,
            instruction :parsed :info :authority :: string
        ) AS mint_authority,
        instruction :parsed :info :signers :: string AS signers,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        event_type IN (
            'mintTo',
            'initializeMint',
            'mintToChecked',
            'initializeMint2',
            'initializeConfidentialTransferMint',
            'initializeNonTransferableMint'
        )
        {% if is_incremental() %}
            AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
        {% else %}
            AND _inserted_timestamp::date between '2022-08-12' and '2022-09-05'
        {% endif %}
    UNION
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        instruction_index as index,
        inner_index,
        event_type,
        instruction :parsed :info :mint :: STRING AS mint,
        instruction :parsed :info :account :: STRING as token_account, 
        instruction :parsed :info :decimals :: INTEGER AS DECIMAL,
        COALESCE(
            instruction :parsed :info :amount :: INTEGER,
            instruction :parsed :info :tokenAmount: amount :: INTEGER
        ) AS mint_amount,
        COALESCE(
            instruction :parsed :info :mintAuthority :: string,
            instruction:parsed :info :multisigMintAuthority :: string,
            instruction :parsed :info :authority :: string
        ) AS mint_authority,
        instruction :parsed :info :signers :: string AS signers,
        _inserted_timestamp
    FROM
        {{ ref('silver__events_inner') }}
    WHERE
        instruction :parsed :type :: STRING IN (
            'mintTo',
            'initializeMint',
            'mintToChecked',
            'initializeMint2',
            'initializeConfidentialTransferMint',
            'initializeNonTransferableMint'
        )
        {% if is_incremental() %}
            AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
        {% else %}
            AND _inserted_timestamp::date between '2022-08-12' and '2022-09-05'
        {% endif %}
)

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    event_type,
    mint,
    token_account,
    DECIMAL,
    mint_amount,
    CASE
        WHEN event_type IN (
            'initializeConfidentialTransferMint',
            'initializeNonTransferableMint'
        ) THEN FIRST_VALUE(mint_authority) ignore nulls over (
            PARTITION BY tx_id
            ORDER BY
                CASE
                    WHEN mint_authority IS NOT NULL THEN 0
                    ELSE 1
                END,
                INDEX
        )
        ELSE mint_authority
    END AS mint_authority,
    signers,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'index', 'inner_index', 'mint']
    ) }} AS mint_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    prefinal