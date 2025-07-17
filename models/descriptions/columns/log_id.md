{% docs log_id %}

A unique identifier for the swap event, typically a combination of the transaction ID (TX_ID) and the event index within the transaction. This field enables precise event-level analytics and deduplication.

- **Data type:** STRING (composite key, e.g., '5G7...:3')
- **Business context:** Used to uniquely identify swap events, prevent double-counting, and join with event-level logs.
- **Analytics use cases:** Event-level deduplication, log/event joins, and transaction traceability.
- **Example:** '5G7...:3'

{% enddocs %} 