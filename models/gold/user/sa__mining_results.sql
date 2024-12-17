{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(wallet)'
    ),
    tags = ['daily']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_modified_query %}

SELECT
    MAX(
        DATE
    ) AS bd
FROM
    {{ this }}

    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query) [0] [0] %}
{% endif %}
{% endif %}
SELECT
    block_timestamp :: DATE AS DATE,
    CASE
        WHEN signers [0] = 'FEEPAYye6DtaJhSXFR65ENXMroiseL1jqYT4QxHKRoZr' THEN signers [1]
        ELSE signers [0]
    END :: STRING AS wallet,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('qe84yNaK76yaZqGqyWJ9A1Npgnt4NSzicbcZvxEQuJy') THEN VALUE :parsed :info :amount
        END
    ) AS arco_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('CHxH1BkLp5A6VcQFCYqKGW88i5XFJV5KyyxewSiwcbkB') THEN VALUE :parsed :info :amount
        END
    ) AS biomass_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('6ny545QXZbGDWXo6F7UEVC7MncAzNNxB62tbVoYWG5gs') THEN VALUE :parsed :info :amount
        END
    ) AS carbon_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('4rZNPtbe9kD44RNdf5XxB6RBWsWaHTg76esbrkWGpkXU') THEN VALUE :parsed :info :amount
        END
    ) AS cuore_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('9LPyad7NE1VDApsirv46gMrRhbmsbFGMMy5CUiAQXkfP') THEN VALUE :parsed :info :amount
        END
    ) AS diamond_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('FpTUZKuviuGaww6ijjXdoeuJtFeEjabEXnzxRYHukhMx') THEN VALUE :parsed :info :amount
        END
    ) AS hyg_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('2mCjq4ST1Svtn4f61cCvoQmQcZ9Zk5S1LcyCZKgwfmae') THEN VALUE :parsed :info :amount
        END
    ) AS feore_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('4PxrWv9WGmbXTSwpkP8kKASrH8VGt2Zd53ePvVmQMoG5') THEN VALUE :parsed :info :amount
        END
    ) AS lumanite_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('FiSSigWUWGir9mTLpzRVm8XXXy1SAju6Ehcvb3xJP2iF') THEN VALUE :parsed :info :amount
        END
    ) AS nitro_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('6fzSXrr9TGJVYcLjZz9W86Ac4DdG3dmKbNhRrmeRRq9g') THEN VALUE :parsed :info :amount
        END
    ) AS rochinol_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('F6JHyw2XqCL4TsbAYZfKU1CPrx74webLCPkoDSgHAg7c') THEN VALUE :parsed :info :amount
        END
    ) AS sand_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('7vLhbhh56tY1eHqdskpz3Uid85zd6Pi2jjGx9HRm5Z3o') THEN VALUE :parsed :info :amount
        END
    ) AS tiore_mined,
    SUM(
        CASE
            WHEN VALUE :parsed :info :authority IN ('C2478tbSLC1gfcDuCyr4pv66QQiybn77EiR1a4k7htT5')
            AND VALUE :parsed :type = 'transfer' THEN VALUE :parsed :info :amount
        END
    ) AS sdu_collected,
    {{ dbt_utils.generate_surrogate_key(
        ['wallet', 'date']
    ) }} AS mining_results_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__events') }},
    LATERAL FLATTEN(
        input => inner_instruction :instructions
    )
WHERE
    program_id IN ('SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE')
    AND VALUE :parsed :info :authority IN (
        'qe84yNaK76yaZqGqyWJ9A1Npgnt4NSzicbcZvxEQuJy',
        'CHxH1BkLp5A6VcQFCYqKGW88i5XFJV5KyyxewSiwcbkB',
        '6ny545QXZbGDWXo6F7UEVC7MncAzNNxB62tbVoYWG5gs',
        '4rZNPtbe9kD44RNdf5XxB6RBWsWaHTg76esbrkWGpkXU',
        '9LPyad7NE1VDApsirv46gMrRhbmsbFGMMy5CUiAQXkfP',
        'FpTUZKuviuGaww6ijjXdoeuJtFeEjabEXnzxRYHukhMx',
        '2mCjq4ST1Svtn4f61cCvoQmQcZ9Zk5S1LcyCZKgwfmae',
        '4PxrWv9WGmbXTSwpkP8kKASrH8VGt2Zd53ePvVmQMoG5',
        'FiSSigWUWGir9mTLpzRVm8XXXy1SAju6Ehcvb3xJP2iF',
        '6fzSXrr9TGJVYcLjZz9W86Ac4DdG3dmKbNhRrmeRRq9g',
        'F6JHyw2XqCL4TsbAYZfKU1CPrx74webLCPkoDSgHAg7c',
        '7vLhbhh56tY1eHqdskpz3Uid85zd6Pi2jjGx9HRm5Z3o',
        'C2478tbSLC1gfcDuCyr4pv66QQiybn77EiR1a4k7htT5'
    )
    AND instruction :accounts [11] = 'foodQJAztMzX1DKpLaiounNe2BDMds5RNuPC6jsNrDG'
    AND instruction :accounts [12] = 'ammoK8AkX2wnebQb35cDAZtTkvsXQbi82cGeTnUvvfK'
    AND instruction :accounts [18] = 'GAMEzqJehF8yAnKiTARUuhZMvLvkZVAsCVri5vSfemLr'
    AND VALUE :parsed :type = 'transfer'
    AND succeeded
    AND wallet IN (
        SELECT
            wallet
        FROM
            {{ ref('sa__mining_fleets') }}
    )
    AND block_timestamp >= '2024-04-01'

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{ max_modified_timestamp }}'
{% endif %}
GROUP BY
    1,
    2
