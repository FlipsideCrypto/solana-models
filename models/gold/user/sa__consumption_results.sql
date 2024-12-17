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
            WHEN instruction :parsed :info :mint :: STRING = 'ARCoQ9dndpg6wE2rRexzfwgJR3NoWWhpcww3xQcQLukg'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS arco_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'MASS9GqtJz6ABisAxcUn3FeR4phMqH1XfG6LPKJePog'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS biomass_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'CARBWKWvxEuMcq3MqCxYfi7UoFVpL9c4rsQS99tw6i4X'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS carbon_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'CUore1tNkiubxSwDEtLc3Ybs1xfWLs8uGjyydUYZ25xc'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS cuore_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'DMNDKqygEN3WXKVrAD4ofkYBc4CKNRhFUbXP4VK7a944'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS diamond_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'HYDR4EPHJcDPcaLYUcNCtrXUdt1PnaN4MvE655pevBYp'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS hyg_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'FeorejFjRRAfusN9Fg3WjEZ1dRCf74o6xwT5vDt3R34J'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS feore_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'LUMACqD5LaKjs1AeuJYToybasTXoYQ7YkxJEc4jowNj'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS lumanite_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'Nitro6idW5JCb2ysUPGUAvVqv3HmUR7NVH7NdybGJ4L'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS nitro_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'RCH1Zhg4zcSSQK8rw2s6rDMVsgBEWa4kiv1oLFndrN5'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS rochinol_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'SiLiCA4xKGkyymB5XteUVmUeLqE4JGQTyWBpKFESLgh'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS sand_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'tiorehR1rLfeATZ96YoByUkvNFsBfUUSQWgSH2mizXL'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS tiore_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'aeroBCMu6AX6bCLYd1VQtigqZh8NGSjn54H1YSczHeJ'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS agel_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'ammoK8AkX2wnebQb35cDAZtTkvsXQbi82cGeTnUvvfK'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS ammo_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'CPPRam7wKuBkYzN5zCffgNU17RKaeMEns4ZD83BqBVNR'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS copper_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'cwirGHLB2heKjCeTy4Mbp4M443fU4V7vy2JouvYbZna'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS cprwire_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'CRYSNnUd7cZvVfrEVtVNKmXiCPYdZ1S5pM5qG2FDVZHF'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS cryslat_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'EMAGoQSP89CJV5focVjrpEuE4CeqJ4k1DouQW7gUu7yX'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS elecmag_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'ELECrjC8m9GxCqcm4XCNpFvkS8fHStAvymS6MJbe3XLZ'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS electro_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'SUBSVX9LYiPrzHeg2bZrqFSDSKkrQkiCesr6SjtdHaX'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS enrgsub_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'FiELD9fGaCgiNMfzQKKZD78wxwnBHTwjiiJfsieb6VGb'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS fstab_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'foodQJAztMzX1DKpLaiounNe2BDMds5RNuPC6jsNrDG'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS food_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'FMWKb7YJA5upZHbu5FjVRRoxdDw2FYFAu284VqUGF9C2'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS frmwrk_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'fueL3hBZjLLLJHiFH9cqZoozTG3XQZ53diwFPwbzNim'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS fuel_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'GRAPHKGoKtXtdPBx17h6fWopdT5tLjfAP8cDJ1SvvDn4'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS graph_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'HYCBuSWCJ5ZEyANexU94y1BaBPtAX2kzBgGD2vES2t6M'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS hcrb_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'ironxrUhTEaBiR9Pgp6hy4qWx6V2FirDoXhsFP25GFP'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS iron_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'MAGNMDeDJLvGAnriBvzWruZHfXNwWHhxnoNF75AQYM5'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS magnet_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'PTCLSWbwZ3mqZqHAporphY2ofio8acsastaHfoP87Dc'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS prtacl_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'PoLYs2hbRt5iDibrkPT9e6xWuhSS45yZji5ChgJBvcB'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS polymer_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'PoWRYJnw3YDSyXgNtN3mQ3TKUMoUSsLAbvE8Ejade3u'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS pwrsrc_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'RABSXX6RcqJ1L5qsGY64j91pmbQVbsYRQuw1mmxhxFe'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS radabs_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'STEELXLJ8nfJy3P4aNuGxyNRbWPohqHSwxY75NsJRGG'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS steel_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'EMiTWSLgjDVkBbLFaMcGU6QqFWzX9JX6kqs1UtUjsmJA'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS strange_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'CoNDDRCNxXAMGscCdejioDzb6XKxSzonbWb36wzSgp5T'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS sprcond_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'TTNM1SMkM7VKtyPW6CNBZ4cg3An3zzQ8NVLS2HpMaWL'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS ttnm_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'tooLsNYLiVqzg8o4m3L2Uetbn62mvMWRqkog6PQeYKL'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS tool_burn_amount,
    SUM(
        CASE
            WHEN instruction :parsed :info :mint :: STRING = 'SDUsgfSZaDhhZ76U3ZgvtFiXsfnHbf2VrzYxjBZ5YbM'
            AND instruction :parsed :type = 'burn' THEN instruction :parsed :info :amount
        END
    ) AS sdu_burn_amount,
    {{ dbt_utils.generate_surrogate_key(
        ['wallet', 'date']
    ) }} AS consumption_results_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__events_inner') }}
WHERE
    instruction_program_id IN ('SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE')
    AND succeeded
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
