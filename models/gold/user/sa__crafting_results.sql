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
            WHEN instruction :parsed :info :authority IN ('22CD73vczVbAQDG8yM8Rz1dxeD7unh46ASMjq8hfNz4e') THEN instruction :parsed :info :amount
        END
    ) AS agel_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('3BgDVYnNzeLAnBKKCsxwrQnkLqCX5hiVgyvTswQEwUrY') THEN instruction :parsed :info :amount
        END
    ) AS ammo_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('Dhk6y4YprcCzZkDEfu2iFP17r12iyE7uJwZrruoRgff2') THEN instruction :parsed :info :amount
        END
    ) AS copper_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('3CHRtAhqCmTyG7f9iKtVcGUsefiRNgebK133pqbF4hKS') THEN instruction :parsed :info :amount
        END
    ) AS cprwire_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('51TU53QarL8bBMnobWYbDkerDiiWTqWjoMWiFFLZ2Hxd') THEN instruction :parsed :info :amount
        END
    ) AS cryslat_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('4zWFbiy7sJSmus6R2oNDVbBtoMLnPZEfux7skW7aGPwr') THEN instruction :parsed :info :amount
        END
    ) AS elecmag_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('9ej2RgtmkcDzkkCrMJe2y8Kr2Rc9GtpgTzusQSqLcQhQ') THEN instruction :parsed :info :amount
        END
    ) AS electro_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('8ydLC44M68b7ZZpiVYAn7FF7rHPMUeEBePZ2s6NYmvTB') THEN instruction :parsed :info :amount
        END
    ) AS enrgsub_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('9yaDYTGDBaJPZLfL2KWuqNyFwQs4qM8h3HBnhG1q8D9s') THEN instruction :parsed :info :amount
        END
    ) AS fstab_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('b3cDzvXTTQNAYdvcEVyTs6YcyHUN9H3KEJurvXEdacv') THEN instruction :parsed :info :amount
        END
    ) AS food_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('8dvios2LKQcKcvWodSZK33MNpp4N2EVpP4iYZhaJHFPL') THEN instruction :parsed :info :amount
        END
    ) AS frmwrk_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('A3UZKrNzgvpqYypjRvSz54GpLVYkF3h5fodkkxmqehax') THEN instruction :parsed :info :amount
        END
    ) AS fuel_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('5AdWeAw1L2T6ABjWDa8EdJ6nrQvrpX1Sjmdjq7b9D5wP') THEN instruction :parsed :info :amount
        END
    ) AS graph_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('9KuqCbKqytB2GzYadsotfUA6Nix3hy4SBCnWEi9SNp5F') THEN instruction :parsed :info :amount
        END
    ) AS hcrb_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('4U7rdLeeEMuux9Bb8PFor7bbWnUFg7uRRWdMLMiNncvt') THEN instruction :parsed :info :amount
        END
    ) AS iron_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('6kokRCRN8yUaKVh4thgunaQ7bSuSCLTW1ixdVXbK1X1m') THEN instruction :parsed :info :amount
        END
    ) AS magnet_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('D5AEX7zzc5f8C5HxwCZByYeuhY2ohLYwzyrtk2Xuq2RZ') THEN instruction :parsed :info :amount
        END
    ) AS prtacl_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('tyU1xaQfTX7mWc6VkV3daGiVhYCUbKfVUjZYMW4cLmM') THEN instruction :parsed :info :amount
        END
    ) AS polymer_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('7pW7cuE4s5kUYnhZRocYqXqHYeNs9jNyZPosQqKciNQ5') THEN instruction :parsed :info :amount
        END
    ) AS pwrsrc_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('7QR4KHRGa5xNQVwdAB94PgDJMptr7ktrJEijKef2Lo5j') THEN instruction :parsed :info :amount
        END
    ) AS radabs_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('AA8gMJvB9fTZb9gXRjGGaQTfxb6DXbAJhAaCyunr1MjX') THEN instruction :parsed :info :amount
        END
    ) AS steel_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('AgBe7NrjzgDjDC9ogQbkba3CmtiJpC9enLb5x56DpQ5J') THEN instruction :parsed :info :amount
        END
    ) AS strange_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('6sEguzy1KBatyz7M3FW2U8Av32VTzEjdc8TmLwa3HTJZ') THEN instruction :parsed :info :amount
        END
    ) AS sprcond_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('9PX4gMtq1G7qiurYZQCUCqmFrmFWQVfxiDCRB2ummdTW') THEN instruction :parsed :info :amount
        END
    ) AS ttnm_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :authority IN ('AkPq1AaBcveGUtqyN8AAoTrvaLFUWS6zmF5NbDCvN5m6') THEN instruction :parsed :info :amount
        END
    ) AS tool_amount,
    {{ dbt_utils.generate_surrogate_key(
        ['wallet', 'date']
    ) }} AS crafting_results_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__events_inner') }}
WHERE
    instruction_program_id IN ('SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE')
    AND succeeded
    AND instruction :parsed :type = 'transfer'
    AND wallet IN (
        SELECT
            wallet
        FROM
            {{ ref('sa__profile_wallets') }}
    )
    AND block_timestamp >= '2024-04-01'

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{ max_modified_timestamp }}'
{% endif %}
GROUP BY
    1,
    2
