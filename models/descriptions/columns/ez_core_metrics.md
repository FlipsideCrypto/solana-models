{% docs ez_core_metrics_hourly_table_doc %}

A convenience table that aggregates block and transaction related metrics using various aggregate functions such as SUM, COUNT, MIN and MAX from the fact_transactions table, on an hourly basis. Stats for the current hour will be updated as new data arrives.

{% enddocs %}

{% docs block_timestamp_hour %}

The hour of the timestamp of the block.

{% enddocs %}

{% docs block_number_min %}

The minimum block number in the hour.

{% enddocs %}

{% docs block_number_max %}

The maximum block number in the hour.

{% enddocs %}

{% docs block_count %}

The number of blocks in the hour.

{% enddocs %}

{% docs transaction_count %}

The number of transactions in the hour.

{% enddocs %}

{% docs transaction_count_success %}

The number of successful transactions in the hour.

{% enddocs %}

{% docs transaction_count_failed %}

The number of failed transactions in the hour.

{% enddocs %}

{% docs total_fees_native %}

The sum of all fees in the hour, in Lamports.

{% enddocs %}

{% docs total_fees_usd %}

The sum of all fees in the hour, in USD.

{% enddocs %}

{% docs unique_signers_count %}

The count of unique first signers for transactions in the hour.

{% enddocs %}