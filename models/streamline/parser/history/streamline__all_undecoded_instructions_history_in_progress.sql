{{ config (
    materialized = 'incremental',
    cluster_by = "ROUND(block_id, -6)",
    full_refresh = false,
    tags = ['streamline'],
) }}

SELECT
    *,
    sysdate() as _inserted_timestamp
FROM
    {{ ref('streamline__all_undecoded_instructions_history_queue') }}
LIMIT
    0
