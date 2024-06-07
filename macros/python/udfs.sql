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

{% macro create_udf_get_compute_units_consumed(schema) %}
create or replace function {{ schema }}.udf_get_compute_units_consumed(log_messages array, instructions array)
returns int
language python 
runtime_version = '3.8'
handler = 'get_compute_units_consumed'
as 
$$
def get_compute_units_consumed(log_messages, instructions):
  import re
  units_consumed_list = []
  selected_logs = set()
  for instr in instructions:
    program_id = instr['programId']
    for logs in log_messages:
      if logs in selected_logs:
        continue
      if re.search(f"Program {program_id} consumed", logs):
        units_consumed = int(re.findall(r'consumed (\d+)', logs)[0])
        units_consumed_list.append(units_consumed)
        selected_logs.add(logs)
        break
  total_units_consumed = sum(units_consumed_list)
  return None if total_units_consumed == 0 else total_units_consumed
$$;
{% endmacro %}

{% macro create_udf_get_compute_units_total(schema) %}
create or replace function {{ schema }}.udf_get_compute_units_total(log_messages array, instructions array)
returns int
language python 
runtime_version = '3.8'
handler = 'get_compute_units_total'
as 
$$
def get_compute_units_total(log_messages, instructions):
  import re
  match = None
  for instr in instructions:
    program_id = instr['programId']
    for logs in log_messages:
      match = re.search(f"Program {program_id} consumed \d+ of (\d+) compute units", logs)
      if match:
        total_units = int(match.group(1))
        return total_units
  if match is None:
    return None
$$;
{% endmacro %}

{% macro create_udf_get_tx_size(schema) %}
create or replace function {{ schema }}.udf_get_tx_size(accts array, instructions array, version string, addr_lookups array, signers array)
returns int
language python
runtime_version = '3.8'
handler = 'get_tx_size' 
AS
$$
def get_tx_size(accts, instructions, version, addr_lookups, signers) -> int:

    header_size = 3
    n_signers = len(signers)
    n_pubkeys = len(accts)
    n_instructions = len(instructions)
    signature_size = (1 if n_signers <= 127 else (2 if n_signers <= 16383 else 3)) + (n_signers * 64)
    if version == '0':
        version_size = 1
        v0_non_lut_accts_size = len([acct for acct in accts if acct.get('source') == 'transaction'])
        account_pubkeys_size = (1 if n_pubkeys <= 127 else (2 if n_pubkeys <= 16383 else 3)) + (v0_non_lut_accts_size * 32)
    else:
        version_size = 0
        account_pubkeys_size = (1 if n_pubkeys <= 127 else (2 if n_pubkeys <= 16383 else 3)) + (n_pubkeys * 32)
    blockhash_size = 32
    program_id_index_size = (1 if n_instructions <= 127 else (2 if n_instructions <= 16383 else 3)) + (n_instructions)
    accounts_index_size = sum((1 if len(instruction.get('accounts', [])) <= 127 else (2 if len(instruction.get('accounts', [])) <= 16383 else 3)) + len(instruction.get('accounts', [])) for instruction in instructions)

    address_lookup_size = 0
    if version == '0' and addr_lookups:
        total_items = len(addr_lookups)
        readonly_items = sum(len(item.get('readonlyIndexes', [])) for item in addr_lookups)
        writeable_items = sum(len(item.get('writableIndexes', [])) for item in addr_lookups)
        address_lookup_size = (total_items * 34) + readonly_items + writeable_items
        address_lookup_size = (1 if address_lookup_size <= 127 else (2 if address_lookup_size <= 16383 else 3)) + address_lookup_size


    data_size = 0
    base58_chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    base58_map = {c: i for i, c in enumerate(base58_chars)}
    
    for instruction in instructions:
        bi = 0
        leading_zeros = 0
        data_base58 = instruction.get('data', b'')
        for c in data_base58:
            if c not in base58_map:
                raise ValueError('Invalid character in Base58 string')
            bi = bi * 58 + base58_map[c]

        hex_str = hex(bi)[2:]
        if len(hex_str) % 2 != 0:
            hex_str = '0' + hex_str

        for c in data_base58:
            if c == '1':
                leading_zeros += 2
            else:
                break
        

        temp_data_size = len('0' * leading_zeros + hex_str)
        data_size += (1 if temp_data_size / 2 <= 127 else (2 if temp_data_size / 2 <= 16383 else 3)) + (temp_data_size / 2)
    
    for instruction in instructions:
        if 'data' not in instruction:
            parsed = instruction.get('parsed')
            if isinstance(parsed, dict):
                type_ = parsed.get('type')
            else:
                type_ = None
        
            if type_ == 'transfer' and instruction.get('program') == 'spl-token':
                data_size += 7
                accounts_index_size += 4
            elif instruction.get('program') == 'spl-memo' and instruction.get('programId') == 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr':
                data_size += 30
                accounts_index_size += 0
            elif type_ == 'transfer' and instruction.get('program') == 'system':
                data_size += 9
                accounts_index_size += 3
            elif instruction.get('program') == 'spl-memo' and instruction.get('programId') == 'Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo':
                data_size += 43
                accounts_index_size += 0
            elif type_ == 'transferChecked' and instruction.get('program') == 'spl-token':
                data_size += 8
                accounts_index_size += 5
            elif type_ == 'write' and instruction.get('program') == 'bpf-upgradeable-loader':
                info = parsed.get('info')
                if info:
                  bytes_data = info.get('bytes')
                  if bytes_data:
                    data_size += len(bytes_data) / 2
        
    final_data_size = data_size

    transaction_size = (
        header_size + account_pubkeys_size + blockhash_size +
        signature_size + program_id_index_size + accounts_index_size +
        final_data_size + address_lookup_size + version_size
    )

    return transaction_size

$$;
{% endmacro %}

{% macro create_udf_get_account_pubkey_by_name(schema) %}
create or replace function {{ schema }}.udf_get_account_pubkey_by_name(name string, accounts array)
returns string
language python
runtime_version = '3.8'
handler = 'get_account_pubkey_by_name'
as
$$
def get_account_pubkey_by_name(name, accounts) -> str:
    if accounts is None:
        return None
    for i,a in enumerate(accounts):
        if a and a.get("name","").lower() == name.lower():
            return a.get("pubkey")

    return None
$$;
{% endmacro %}

{% macro create_udf_get_logs_program_data(schema) %}
create or replace function {{ schema }}.udf_get_logs_program_data(logs array)
returns array
language python
runtime_version = '3.8'
handler = 'get_logs_program_data'
AS
$$
import re

def get_logs_program_data(logs) -> list:
    program_data = []
    parent_event_type = ""
    child_event_type = ""
    parent_index = -1
    child_index = None
    current_ancestry = []

    pattern = re.compile(r'invoke \[(?!1\])\d+\]')

    try:
        for i, log in enumerate(logs):
            if log.endswith(" invoke [1]"):
                program = log.replace("Program ","").replace(" invoke [1]","")
                parent_index += 1

                if i+1 < len(logs) and logs[i+1].startswith("Program log: Instruction: "):
                    parent_event_type = logs[i+1].replace("Program log: Instruction: ","")
                elif i+1 < len(logs) and logs[i+1].startswith("Program log: IX: "):
                    parent_event_type = logs[i+1].replace("Program log: IX: ","")
                else:
                    parent_event_type = "UNKNOWN"

                current_ancestry = [(program,None,parent_event_type)]
            elif bool(pattern.search(log)):
                child_index = child_index+1 if child_index is not None else 0
                current_program = pattern.sub('', log.replace("Program ","")).strip()
                current_node = int(pattern.search(log)[0].replace("invoke [","").replace("]",""))

                if i+1 < len(logs) and logs[i+1].startswith("Program log: Instruction: "):
                    child_event_type = logs[i+1].replace("Program log: Instruction: ","")
                elif i+1 < len(logs) and logs[i+1].startswith("Program log: IX: "):
                    child_event_type = logs[i+1].replace("Program log: IX: ","")
                else:
                    child_event_type = "UNKNOWN"

                if len(current_ancestry) >= current_node:
                    current_ancestry[current_node-1] = (current_program, child_index, child_event_type)
                else:
                    current_ancestry.append((current_program, child_index, child_event_type))
            
            if log.endswith(" success"):
                current_program_id = current_ancestry[-1][0]
                current_event_type = current_ancestry[-1][2]
                current_index = parent_index
                current_inner_index = current_ancestry[-1][1]
                current_ancestry.pop()
            else:
                current_program_id = current_ancestry[-1][0]
                current_event_type = current_ancestry[-1][2]
                current_index = parent_index
                current_inner_index = current_ancestry[-1][1]
            
            if log.startswith("Program data: "):
                data = log.replace("Program data: ","")
                program_data.append({"data": data, 
                                    "program_id": current_program_id, 
                                    "index": current_index, 
                                    "inner_index": current_inner_index, 
                                    "event_type": current_event_type})
    except Exception as e:
        message = f"error trying to parse logs {e}"
        return [{"error": message}]
    
    return program_data if len(program_data) > 0 else None
$$;
{% endmacro %}