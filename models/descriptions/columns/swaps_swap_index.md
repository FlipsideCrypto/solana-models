{% docs swaps_swap_index %}

The order in which the intermediate swap was executed within a routed transaction (e.g., Jupiter aggregator route). Used to reconstruct the full path of multi-hop swaps.

- **Data type:** INTEGER (0-based index)
- **Business context:** Enables step-by-step analysis of routed swaps, DEX path optimization, and route efficiency studies.
- **Analytics use cases:** Route reconstruction, DEX path analysis, and aggregator performance benchmarking.
- **Example:** In a 3-hop Jupiter route, swaps will have indices 0, 1, and 2.

{% enddocs %}