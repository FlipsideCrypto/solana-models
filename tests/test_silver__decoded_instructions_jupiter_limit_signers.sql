SELECT DISTINCT 
    signers[0]::string AS signer
FROM
    {{ ref('silver__decoded_instructions_combined') }} 
WHERE 
    program_id in ('j1o2qRpjcyUwEvwtcfhEQefh773ZgjxcVRry7LDqg5X','jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu')
    AND event_type = 'flashFillOrder'
    AND _inserted_timestamp >= current_date - 7
    AND signer NOT IN ('j1oAbxxiDUWvoHxEDhWE7THLjEkDQW2cSHYn2vttxTF',
        'Gw9QoW4y72hFDVt3RRzyqcD4qrV4pSqjhMMzwdGunz6H',
        'LoAFmGjxUL84rWHk4X6k8jzrw12Hmb5yyReUXfkFRY6',
        '71WDyyCsZwyEYDV91Qrb212rdg6woCHYQhFnmZUBxiJ6',
        'EccxYg7rViwYfn9EMoNu7sUaV82QGyFt6ewiQaH1GYjv',
        'j1oeQoPeuEDmjvyMwBmCWexzCQup77kbKKxV59CnYbd',
        'JTJ9Cz7i43DBeps5PZdX1QVKbEkbWegBzKPxhWgkAf1',
        'j1opmdubY84LUeidrPCsSGskTCYmeJVzds1UWm6nngb',
        'AfQ1oaudsGjvznX4JNEw671hi57JfWo4CWqhtkdgoVHU'
    )