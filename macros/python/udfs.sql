{% macro create_udf_ordered_signers(schema) %}
create or replace function {{ schema }}.udf_ordered_signers(accts array)
returns array
language python
runtime_version = '3.8'
handler = 'ordered_signers'
as
$$
def ordered_signers(accts) -> list:
    signers = [] 
    for v in accts:
        if v["signer"]:
            signers.append(v["pubkey"])

    return signers
$$;
{% endmacro %}

{% macro create_udf_get_all_inner_instruction_events(schema) %}
create or replace function {{ schema }}.udf_get_all_inner_instruction_events(inner_instruction array)
returns array
language python
runtime_version = '3.8'
handler = 'get_all_inner_instruction_events'
as
$$
def get_all_inner_instruction_events(inner_instruction) -> list:
    event_types = [] 
    if inner_instruction:
        for v in inner_instruction:
            if type(v) is dict and v.get("parsed") and type(v["parsed"]) is dict and v["parsed"].get("type"):
                event_types.append(v["parsed"]["type"])
            else:
                event_types.append(None)

    return event_types
$$;
{% endmacro %}

{% macro create_udf_get_account_balances_index(schema) %}
create or replace function {{ schema }}.udf_get_account_balances_index(account string, account_keys array)
returns int
language python
runtime_version = '3.8'
handler = 'get_account_balances_index'
as
$$
def get_account_balances_index(account, account_keys) -> int:
    for i,a in enumerate(account_keys):
        if a and a.get("pubkey") == account:
            return i

    return None
$$;
{% endmacro %}

{% macro create_udf_get_all_inner_instruction_program_ids(schema) %}
create or replace function {{ schema }}.udf_get_all_inner_instruction_program_ids(inner_instruction variant)
returns array
language python
runtime_version = '3.8'
handler = 'get_all_inner_instruction_program_ids'
as
$$
def get_all_inner_instruction_program_ids(inner_instruction) -> list:
    program_ids = [] 
    if inner_instruction:
        for v in inner_instruction.get('instructions',[]):
            if type(v) is dict and v.get("programId"):
                program_ids.append(v.get("programId"))
            else:
                program_ids.append(None)

    return program_ids
$$;
{% endmacro %}

{% macro create_udf_get_multi_signers_swapper(schema) %}
create or replace function {{ schema }}.udf_get_multi_signers_swapper(tx_to array, tx_from array, signers array)
returns string
language python
runtime_version = '3.8'
handler = 'get_multi_signers_swapper'
as
$$
def get_multi_signers_swapper(tx_to, tx_from, signers):
    lst = tx_to + tx_from
    d = {}
    for v in lst:
        d[v] = d[v]+1  if d.get(v) else 1
    
    cnts = sorted(d.items(), key = lambda x: x[1], reverse = True)

    for v in cnts:
        for signer in signers:
            if v[0] == signer:
                return signer
                
    return signers[0]
$$;
{% endmacro %}

{% macro create_udf_get_jupv4_inner_programs(schema) %}
create or replace function {{ schema }}.udf_get_jupv4_inner_programs(inner_instruction array)
returns array
language python
runtime_version = '3.8'
handler = 'get_jupv4_inner_programs'
as
$$
def get_jupv4_inner_programs(inner_instruction) -> list:
    inner_programs = [] 
    if inner_instruction:
        for i, v in enumerate(inner_instruction):
            if type(v) is dict and v.get("programId") not in ['TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA','11111111111111111111111111111111']:
                inner_programs.append({
                    "inner_index": i,
                    "program_id": v.get("programId")
                })

    return inner_programs
$$;
{% endmacro %}


{% macro create_udf_get_tx_size_test(schema) %}
create or replace function solana_dev.silver.udf_get_tx_size_test(accts array, instructions array, version string, addr_lookups array)
returns int
language python
runtime_version = '3.8'
handler = 'get_tx_size_test'
as
$$
def get_tx_size_test(accts, instructions, version, addr_lookups) -> int:

    --3 bytes for msg header
    msg_header_size = 3
    --32 bytes per account pubkey
    account_pubkeys_size = len(accts) * 32
    --32 bytes for recent blockhash
    blockhash_size = 32
    
    --64 bytes per signature
    signers_ct_temp = 0
    for v in accts:
        if v["signer"]:
            signers_ct_temp += 1
            
    sig_size = signers_ct_temp * 64
    --1 byte for program id index (1 for each instruction)
    program_id_idx_size = len(instructions)
    --1 byte for each item in accounts array
    accounts_idx_size = sum(len(instruction.get('accounts', [])) for instruction in instructions)
    --1 byte per character in 'data' string
    data_size = sum(len(instruction.get('data', b'')) for instruction in instructions)

    -- 1 byte per index in 'address_table_lookups' + 1 byte per writableIndexes + 1 byte per readonlyIndexes
    address_lookup_size = 0
    if version == '0':
        total_items = 0
        readonly_items = 0
        writeable_items = 0
        for item in addr_lookups:
            total_items += 1
            readonly_items += len(item.get('readonlyIndexes', []))
            writeable_items += len(item.get('writableIndexes', []))
        address_lookup_size = total_items + readonly_items + writeable_items

    transaction_size = (
        msg_header_size + account_pubkeys_size + blockhash_size +
        sig_size + program_id_idx_size + accounts_idx_size +
        data_size + address_lookup_size
    )
    
    return transaction_size
$$;
{% endmacro %}