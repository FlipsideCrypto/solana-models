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
instructs AS (
    SELECT 
        tx_id, 
        block_timestamp, 
        signers, 
        index, 
        fee, 
        i.value :programId :: STRING as program_id, 
        i.value :parsed :info :authority :: STRING as authority
    FROM 
        {{ ref('silver__transactions2') }} t, 
        TABLE(FLATTEN(instructions)) i
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
), 
signer_array AS (
  SELECT 
     tx_id, 
     fee, 
     s.value :: STRING as signer
  FROM instructs,
  TABLE(FLATTEN(signers)) s
  WHERE 
      authority IS NOT NULL 
  AND s.value = authority
),
indices AS (
    SELECT 
        tx_id, 
        max(index) AS max_index,
        min(index) AS min_index
    FROM instructs
    GROUP BY tx_id 
),
min_programs AS (
    SELECT 
        t.tx_id,  
        block_timestamp, 
        program_id AS min_program
    FROM 
        instructs t
    LEFT OUTER JOIN indices i
    ON t.tx_id = i.tx_id 
    WHERE t.index = i.min_index
), 
max_programs AS (
    SELECT 
        t.tx_id,  
        block_timestamp, 
        program_id AS max_program
    FROM 
        instructs t
    LEFT OUTER JOIN indices i
    ON t.tx_id = i.tx_id 
    WHERE t.index = i.max_index
),
txs AS (
  SELECT 
      max.tx_id, 
      s.signer, 
      fee, 
      max.block_timestamp, 
      max_program, 
      min_program
  FROM max_programs max

  INNER JOIN signer_array s
  ON max.tx_id = s.tx_id

  INNER JOIN min_programs min
  ON max.tx_id = min.tx_id
)
SELECT 
    signer, 
    min(block_timestamp :: DATE) AS first_tx_date, 
    max(block_timestamp :: DATE) AS last_tx_date, 
    min(min_program) AS first_program_used, 
    max(max_program) AS last_program_used, 
    count(DISTINCT block_timestamp :: date) AS num_days_active, 
    count(DISTINCT tx_id) AS total_txs, 
    count(DISTINCT max_program, min_program) AS programs_used, 
    sum(fee) AS total_fees
FROM txs
WHERE 
    min_program AND max_program NOT IN (
        SELECT 
            address
        FROM 
            programs
    )
GROUP BY signer