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
