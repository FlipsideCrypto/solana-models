{{ config(
  materialized = 'incremental',
  unique_key = "signer",
  incremental_strategy = 'delete+insert',
  cluster_by = 'signer'
) }}

WITH programs AS (
    SELECT 
        address 
    FROM 
        {{ ref('core__dim_labels') }}
    WHERE 
        label_type = 'chadmin'
), 

all_txs AS (
    SELECT 
        tx_id, 
        signers[0] :: STRING AS signer, 
        block_timestamp, 
        instructions[0] :programId :: STRING as program_id, 
        fee, 
        _inserted_timestamp
    FROM 
        {{ ref('silver__transactions') }}
  
    WHERE 
        signers[0] :: STRING = '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
  
    UNION ALL 
    
    SELECT 
        tx_id, 
        signers[0] :: STRING AS signer, 
        block_timestamp, 
        instructions[0] :programId :: STRING as program_id, 
        fee, 
        _inserted_timestamp
    FROM 
        {{ ref('silver__transactions2') }}
    WHERE 
        signers[0] :: STRING = '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
)

SELECT 
    signer, 
    min(block_timestamp :: date) AS first_tx_date, 
    max(block_timestamp :: date) AS last_tx_date, 
    min(program_id) AS first_program_used, 
    max(program_id) AS last_program_used, 
    count(DISTINCT block_timestamp :: date) AS num_days_active, 
    count(DISTINCT tx_id) AS total_txs, 
    count(DISTINCT program_id) AS programs_used, 
    sum(fee) AS total_fees
FROM all_txs t

WHERE 
    signer = '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H'
    AND program_id NOT IN (
        SELECT 
            address
        FROM programs
    )  
GROUP BY signer