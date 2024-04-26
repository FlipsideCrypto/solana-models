{% docs prices_dim_asset_metadata_provider_table_doc %}

A comprehensive dimensional table holding all provider asset metadata and other relevant details pertaining to each id.

{% enddocs %}

{% docs prices_ez_asset_metadata_table_doc %}

A convenience table holding prioritized asset metadata and other relevant details pertaining to each token_address and native asset.

{% enddocs %}

{% docs prices_fact_prices_provider_hourly_table_doc %}

A comprehensive fact table holding id and provider specific open, high, low, close hourly prices.

{% enddocs %}

{% docs prices_ez_prices_hourly_table_doc %}

A convenience table for determining token prices by address and blockchain, and native asset prices by symbol and blockchain.

{% enddocs %}

{% docs prices_provider %}

The provider or source of the data.

{% enddocs %}

{% docs prices_asset_id %}

The unique identifier representing the asset.

{% enddocs %}

{% docs prices_name %}

The name of asset.

{% enddocs %}

{% docs prices_symbol %}

The symbol of asset.

{% enddocs %}

{% docs prices_token_address %}

The specific address representing the asset on a specific platform. This will be NULL if referring to a native asset.

{% enddocs %}

{% docs prices_blockchain %}

The Blockchain, Network, or Platform for this asset.

{% enddocs %}

{% docs prices_blockchain_id %}

The unique identifier of the Blockchain, Network, or Platform for this asset.

{% enddocs %}

{% docs prices_decimals %}

The number of decimals for the asset. May be NULL.

{% enddocs %}

{% docs prices_is_native %}

A flag indicating assets native to the respective blockchain.

{% enddocs %}

{% docs prices_is_deprecated %}

A flag indicating if the asset is deprecated or no longer supported by the provider.

{% enddocs %}

{% docs prices_id_deprecation %}

Deprecating soon! Please use the ASSET_ID column instead.

{% enddocs %}

{% docs prices_hour %}

Hour that the price was recorded at.

{% enddocs %}

{% docs prices_price %}

Closing price of the recorded hour in USD.

{% enddocs %}

{% docs prices_is_imputed %}

A flag indicating if the price was imputed, or derived, from the last arriving record. This is generally used for tokens with low-liquidity or inconsistent reporting.

{% enddocs %}

{% docs prices_open %}

Opening price of the recorded hour in USD.

{% enddocs %}

{% docs prices_high %}

Highest price of the recorded hour in USD

{% enddocs %}

{% docs prices_low %}

Lowest price of the recorded hour in USD

{% enddocs %}

{% docs prices_close %}

Closing price of the recorded hour in USD

{% enddocs %}