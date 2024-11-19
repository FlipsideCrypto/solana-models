{{ config (
    materialized = 'incremental',
    unique_key = 'table_name',
    full_refresh = false,
    tags = ['streamline_decoder'],
) }}

select 
    'placeholder'::string as schema_name,
    'placeholder'::string as table_name