{{ config (
    materialized = 'incremental',
    unique_key = 'program_id',
    full_refresh = false,
    tags = ['streamline_decoder'],
) }}

select 
    'placeholder'::string as program_id,
    {'hello':'world'}::variant as idl,
    'placeholder' as idl_source,
    'flipside' as discord_username,
    sha2(idl) as idl_hash,
    sysdate() as _inserted_timestamp