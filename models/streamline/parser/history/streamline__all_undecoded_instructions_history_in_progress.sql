{{ config (
    materialized = 'incremental',
    full_refresh = false
) }}

SELECT
    *,
    sysdate() as _inserted_timestamp
FROM
    {{ ref('streamline__all_undecoded_instructions_history_queue') }}
LIMIT
    0
