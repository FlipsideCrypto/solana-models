SELECT DISTINCT 
    signers[0]::string AS signer
FROM
    {{ ref('silver__decoded_logs') }} 
WHERE 
    program_id = 'DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M'
    AND event_type = 'Filled'
    AND _inserted_timestamp >= current_date - 7
    AND signer NOT IN ('DCAKxn5PFNN1mBREPWGdk1RXg5aVH9rPErLfBFEi2Emb',
        'DCAK36VfExkPdAkYUQg6ewgxyinvcEyPLyHjRbmveKFw',
        'DCAKuApAuZtVNYLk3KTAVW9GLWVvPbnb5CxxRRmVgcTr'
    )