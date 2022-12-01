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

{% macro create_udf_get_jupv4_inner_programs(schema) %}
create or replace function solana_dev.silver.udf_get_jupv4_inner_programs(inner_instruction array)
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