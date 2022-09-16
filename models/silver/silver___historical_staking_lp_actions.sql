{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH instructs AS (

    SELECT
        tx_id,
        INDEX,
        i.value :parsed :type :: STRING AS event_type,
        i.value :programId :: STRING AS program_id,
        i.value AS instruction
    FROM
        {{ source(
            'bronze_streamline',
            'txs_api'
        ) }},
        TABLE(
            FLATTEN (
                DATA :result :transaction :message :instructions
            )
        ) i
    WHERE
        program_id = 'Stake11111111111111111111111111111111111111'

{% if is_incremental() %}
AND _inserted_date >= CURRENT_DATE - 1
AND TO_TIMESTAMP_NTZ(
    SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER,
    0
) >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION
SELECT
    tx_id,
    CONCAT(
        b.value :index,
        '.',
        C.index
    ) AS INDEX,
    C.value :parsed :type :: STRING AS event_type,
    C.value :programId :: STRING AS program_id,
    C.value AS instruction
FROM
    {{ source(
        'bronze_streamline',
        'txs_api'
    ) }} A,
    TABLE(FLATTEN (DATA :result :meta :innerInstructions)) b,
    TABLE(FLATTEN(b.value :instructions)) C
WHERE
    program_id = 'Stake11111111111111111111111111111111111111'

{% if is_incremental() %}
AND _inserted_date >= CURRENT_DATE - 1
AND TO_TIMESTAMP_NTZ(
    SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER,
    0
) >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
txs AS (
    SELECT
        DISTINCT tx_id
    FROM
        instructs
),
tx_base AS (
    SELECT
        DATA :result :slot AS block_id,
        TO_TIMESTAMP_NTZ(
            DATA :result :blockTime
        ) AS block_timestamp,
        i.tx_id,
        CASE
            WHEN DATA :result :meta :err :: STRING IS NULL THEN TRUE
            ELSE FALSE
        END AS succeeded,
        DATA :result :transaction :message :accountKeys AS account_keys,
        DATA :result :transaction :message :instructions AS instruction,
        DATA :result :meta :innerInstructions AS inner_instruction,
        DATA :result :meta :preBalances AS pre_balances,
        DATA :result :meta :postBalances AS post_balances,
        DATA :result :meta :preTokenBalances AS pre_token_balances,
        DATA :result :meta :postTokenBalances AS post_token_balances,
        TO_TIMESTAMP_NTZ(
            SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER,
            0
        ) AS _inserted_timestamp
    FROM
        {{ source(
            'bronze_streamline',
            'txs_api'
        ) }}
        t
        INNER JOIN txs i
        ON i.tx_id = t.tx_id
{% if is_incremental() %}
WHERE _inserted_date >= CURRENT_DATE - 1
AND TO_TIMESTAMP_NTZ(
    SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER,
    0
) >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    b.tx_id,
    succeeded,
    INDEX,
    event_type,
    program_id,
    [] :: ARRAY AS signers,
    account_keys :: ARRAY AS account_keys,
    i.instruction,
    inner_instruction :: variant AS inner_instruction,
    pre_balances :: ARRAY AS pre_balances,
    post_balances :: ARRAY AS post_balances,
    pre_token_balances :: ARRAY AS pre_token_balances,
    post_token_balances :: ARRAY AS post_token_balances,
    b._inserted_timestamp
FROM
    instructs i
    LEFT OUTER JOIN tx_base b
    ON b.tx_id = i.tx_id
