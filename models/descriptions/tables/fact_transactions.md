{% docs fact_transactions %}

## Description
This table contains one record per transaction on the Solana blockchain, capturing high-level transaction metadata as recorded on-chain. Each record includes block timestamp, transaction identifiers, account and token balance changes, program instructions, and execution metadata for every transaction processed on Solana mainnet. The table covers all finalized transactions, both successful and failed, and is updated as new blocks are processed. Data is sourced from on-chain transaction logs and normalized for analytics.

**IMPORTANT: This is raw transaction data. For most analytics, use the specialized curated tables instead of querying this large table directly.** Transaction components are broken down and extracted into purpose-built tables that are optimized for specific analytical use cases and much easier to work with.

## Key Use Cases
**For most of these use cases, prefer the specialized curated tables listed in "Important Relationships" below:**

- Transaction-level metadata and high-level transaction analytics
- Fee analysis and cost monitoring for Solana transactions (use `fee` and `succeeded` fields)
- Success/failure rate analysis for transactions
- Basic wallet and signer analysis (for detailed analysis, use `core.ez_signers`)
- Building time-series dashboards for network activity at the transaction level

**Use curated tables for these common analyses instead:**
- **Token and asset movement**: Use `core.ez_transfers` (includes USD values, token symbols)
- **Program and instruction analysis**: Use `core.fact_events` and `core.fact_events_inner`
- **Decoded instruction details**: Use `core.ez_events_decoded`
- **Token balance changes**: Use `core.fact_token_balances`

## Important Relationships
**STRONGLY PREFER these curated tables over raw transaction data:**

**For instruction and program analysis:**
- `core.fact_events` - Parsed instruction events and program interactions
- `core.fact_events_inner` - Inner/CPI instruction events for composability analysis
- `core.ez_events_decoded` - Human-readable decoded instruction details with arguments

**For transfer and asset movement analysis:**
- `core.ez_transfers` - Token transfers with USD values, symbols, and verification status
- `core.fact_token_balances` - Token balance changes over time

**For wallet and signer analysis:**
- `core.ez_signers` - Signer addresses extracted and enriched

**Raw transaction relationships:**
- Each transaction links to `core.fact_blocks` via `block_id` for block context
- All curated tables use `tx_id` to link back to transaction context when needed
- Raw fields like `instructions`, `inner_instructions`, and `program_ids` are parsed into the specialized tables above

**Analysis Guidance:**
- **DO NOT** parse `instructions` or `inner_instructions` arrays directly - use `core.fact_events` and `core.fact_events_inner`
- **DO NOT** analyze program IDs from raw data - use the events tables for program interaction analysis  
- **DO NOT** extract transfer data manually - use `core.ez_transfers` for all transfer analytics
- **DO NOT** query this large table for routine analytics - it's optimized as a source table, not for direct analysis

## Commonly-used Fields
**When using this table directly (discouraged for most use cases):**

- `tx_id`: Unique identifier for joins with curated tables and traceability
- `block_id` and `block_timestamp`: For time-series and block-level context
- `fee`: For transaction cost analysis
- `succeeded`: For transaction success/failure rate analysis
- `signers`: For basic signer analysis (prefer `core.ez_signers` for detailed analysis)

**Fields to avoid parsing directly (use curated tables instead):**
- `instructions`, `inner_instructions`: Use `core.fact_events` and `core.fact_events_inner` instead
- `account_keys`, `pre_balances`, `post_balances`: Use `core.ez_transfers` or `core.fact_token_balances` instead
- `log_messages`: Use `core.ez_events_decoded` for parsed program logs

**Reminder: For most analytics, query the specialized curated tables that extract and enrich these raw fields rather than parsing them manually from this table.**

{% enddocs %} 