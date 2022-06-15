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