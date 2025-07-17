{% docs swap_program_id %}

The program ID of the AMM (Automated Market Maker) performing the swap within a Jupiter route. This field identifies the specific DEX protocol executing the swap step.

- **Data type:** STRING (base58 Solana program address)
- **Business context:** Used to identify the DEX protocol for each swap step in a routed transaction.
- **Analytics use cases:** DEX usage analysis within routes, protocol performance comparison, and route optimization studies.
- **Example:** '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'

{% enddocs %} 