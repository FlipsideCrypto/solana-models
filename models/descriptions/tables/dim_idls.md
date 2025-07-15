{% docs dim_idls %}

## Description
This table contains one row per program interface definition (IDL) on the Solana blockchain, but only includes programs for which we have the IDL and are actively decoding instructions. Programs listed here are the only ones for which decoded data is available in downstream models. It maps program IDs to their Interface Definition Language (IDL) documents, hashes, validity, activity status, and submission metadata. Each row represents a unique program interface definition, supporting analytics on program interfaces, upgrades, and developer activity.

## Key Use Cases
- Analyze program interface definitions and upgrades
- Track program activity, validity, and deployment status
- Support analytics on developer activity and protocol upgrades
- Study program adoption, composability, and ecosystem growth
- Enable time-series and event-based analytics on program interfaces

## Important Relationships
- Closely related to `core.fact_decoded_instructions` and `core.ez_events_decoded` (for decoded program calls)
- Use `core.ez_events_decoded` to analyze program calls and usage

## Commonly-used Fields
- `program_id`: For program identification and joins
- `idl`, `idl_hash`: For interface definition and verification
- `is_valid`, `is_active`: For program status analytics
- `last_activity_timestamp`, `date_submitted`: For activity and submission analysis

{% enddocs %} 