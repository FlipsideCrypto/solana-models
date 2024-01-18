SELECT
    block_id,
    tx_id,
    INDEX,
    COALESCE(
        inner_index,
        -1
    )
FROM
    {% if target.database == 'SOLANA' %}
        solana.streamline.complete_decoded_instructions_2
    {% else %}
        solana_dev.streamline.complete_decoded_instructions_2
    {% endif %}
    
WHERE
    _inserted_timestamp between current_date - 2 and current_date - 1
EXCEPT
SELECT
    block_id,
    tx_id,
    INDEX,
    COALESCE(
        inner_index,
        -1
    )
FROM
    {{ ref('silver__decoded_instructions') }}
WHERE
    _inserted_timestamp BETWEEN current_date - 2 and current_date - 1
