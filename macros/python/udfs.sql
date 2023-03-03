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
create or replace function {{ schema }}.udf_get_compute_units_consumed(log_messages array)
returns int
language python 
runtime_version = '3.8'
handler = 'get_compute_units_consumed'
as 
$$
import re
def get_compute_units_consumed(log_messages):
    for record in data:
        tx_id = record['TX_ID']
        logs = record['LOG_MESSAGES']
        consumed_sum = 0
        for i in range(len(logs)):
            consumed = 0
            if "consumed" in logs[i]:
                c = re.findall(r'\b\d+\b', logs[i])
                consumed = int(c[0])
            consumed_sum = consumed_sum + consumed

    return consumed_sum
$$;
{% endmacro %}

{% macro create_udf_get_compute_units_total(schema) %}
create or replace function {{ schema }}.udf_get_compute_units_total(log_messages array)
returns int
language python 
runtime_version = '3.8'
handler = 'get_compute_units_total'
as 
$$
import re
def get_compute_units_total(log_messages):
    for record in data:
        tx_id = record['TX_ID']
        logs = record['LOG_MESSAGES']
        available_sum = 0
        for i in range(len(logs)):
            available = 0
            if "consumed" in logs[i]:
                c = re.findall(r'\b\d+\b', logs[i])
                available = int(c[1])
            available_sum = available_sum + available

    return available_sum
$$;
{% endmacro %}