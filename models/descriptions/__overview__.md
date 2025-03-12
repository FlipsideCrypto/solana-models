{% docs __overview__ %}

# Welcome to the Flipside Crypto Solana Models Documentation

## **What does this documentation cover?**
The documentation included here details the design of the Solana
 tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/) For more information on how these models are built, please see [the github repository.](https://github.com/flipsideCrypto/solana-models/)

## **How do I use these docs?**
The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables (`Solana`.`CORE`.`<table_name>`)

**Core Dimension Tables:**
- [dim_labels](#!/model/model.solana_models.core__dim_labels)
- [dim_idls](#!/model/model.solana_models.core__dim_idls)

**Core Fact Tables:**
- [fact_blocks](#!/model/model.solana_models.core__fact_blocks)
- [fact_events](#!/model/model.solana_models.core__fact_events)
- [fact_transactions](#!/model/model.solana_models.core__fact_transactions)
- [fact_transfers](#!/model/model.solana_models.core__fact_transfers)
- [fact_decoded_instructions](#!/model/model.solana_models.core__fact_decoded_instructions)
- [fact_token_account_owners](#!/model/model.solana_models.core__fact_token_account_owners)

**Core Convenience Tables:**
- [ez_signers](#!/model/model.solana_models.core__ez_signers)
- [ez_events_decoded](#!/model/model.solana_models.core__ez_events_decoded)

### DeFi Tables (`Solana`.`DEFI`.`<table_name>`)
- [ez_dex_swaps](#!/model/model.solana_models.defi__ez_dex_swaps)
- [fact_liquidity_pool_actions](#!/model/model.solana_models.defi__fact_liquidity_pool_actions)
- [fact_stake_pool_actions](#!/model/model.solana_models.defi__fact_stake_pool_actions)
- [fact_swaps](#!/model/model.solana_models.defi__fact_swaps)
- [fact_swaps_jupiter_inner](#!/model/model.solana_models.defi__fact_swaps_jupiter_inner)
- [fact_swaps_jupiter_summary](#!/model/model.solana_models.defi__fact_swaps_jupiter_summary)
- [fact_bridge_activity](#!/model/model.solana_models.defi__fact_bridge_activity)
- [fact_token_burn_actions](#!/model/model.solana_models.defi__fact_token_burn_actions)
- [fact_token_mint_actions](#!/model/model.solana_models.defi__fact_token_mint_actions)

### Governance Tables (`Solana`.`GOV`.`<table_name>`)
- [dim_epoch](#!/model/model.solana_models.gov__dim_epoch)
- [ez_staking_lp_actions](#!/model/model.solana_models.gov__ez_staking_lp_actions)
- [fact_gauges_creates](#!/model/model.solana_models.gov__fact_gauges_create)
- [fact_gauges_votes](#!/model/model.solana_models.gov__fact_gauges_votes)
- [fact_gov_actions](#!/model/model.solana_models.gov__fact_gov_actions)
- [fact_proposal_creation](#!/model/model.solana_models.gov__fact_proposal_creation)
- [fact_proposal_votes](#!/model/model.solana_models.gov__fact_proposal_votes)
- [fact_staking_lp_actions](#!/model/model.solana_models.gov__fact_staking_lp_actions)
- [fact_stake_accounts](#!/model/model.solana_models.gov__fact_stake_accounts)
- [fact_vote_accounts](#!/model/model.solana_models.gov__fact_vote_accounts)
- [fact_block_production](#!/model/model.solana_models.gov__fact_block_production)
- [fact_validators](#!/model/model.solana_models.gov__fact_validators)
- [fact_votes_agg_block](#!/model/model.solana_models.gov__fact_votes_agg_block)
- [fact_rewards_fees](#!/model/model.solana_models.gov__fact_rewards_fees)
- [fact_rewards_rent](#!/model/model.solana_models.gov__fact_rewards_rent)
- [fact_rewards_staking](#!/model/model.solana_models.gov__fact_rewards_staking)
- [fact_rewards_voting](#!/model/model.solana_models.gov__fact_rewards_voting)

### NFT Tables (`Solana`.`NFT`.`<table_name>`)
- [dim_nft_metadata](#!/model/model.solana_models.nft__dim_nft_metadata)
- [fact_nft_mints](#!/model/model.solana_models.nft__fact_nft_mints)
- [fact_nft_sales](#!/model/model.solana_models.nft__fact_nft_sales)
- [ez_nft_sales](#!/model/model.solana_models.nft__ez_nft_sales)
- [fact_nft_burn_actions](#!/model/model.solana_models.nft__fact_nft_burn_actions)
- [fact_nft_mint_actions](#!/model/model.solana_models.nft__fact_nft_mint_actions)

### Price Tables (`Solana`.`PRICE`.`<table_name>`)
- [dim_asset_metadata](#!/model/model.solana_models.price__dim_asset_metadata)
- [fact_prices_ohlc_hourly](#!/model/model.solana_models.price__fact_prices_ohlc_hourly)
- [ez_asset_metadata](#!/model/model.solana_models.price__ez_asset_metadata)
- [ez_prices_hourly](#!/model/model.solana_models.price__ez_prices_hourly)

### Stats Tables (`Solana`.`STATS`.`<table_name>`)
- [ez_core_metrics](#!/model/model.solana_models.stats__ez_core_metrics)

### Marinade Tables (`Solana`.`MARINADE`.`<table_name>`)
- [dim_pools](#!/model/model.solana_models.marinade__dim_pools)
- [ez_liquidity_pool_actions](#!/model/model.solana_models.marinade__ez_liquidity_pool_actions)
- [ez_liquid_staking_actions](#!/model/model.solana_models.marinade__ez_liquid_staking_actions)
- [ez_native_staking_actions](#!/model/model.solana_models.marinade__ez_native_staking_actions)
- [ez_swaps](#!/model/model.solana_models.marinade__ez_swaps)


## **Data Model Overview**

The Solana models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold.**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (core/defi/gov/nft/price): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

### Solana Data

Key notes about our Solana data:
- Transactions before block 39,824,213 (~ 2020-10-07) may not contain BLOCK_TIMESTAMP values, so BLOCK_ID should be utilized when querying older transactions.

## **Using dbt docs**
### Navigation

You can use the ```Project``` and ```Database``` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are *not* shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the ```--models``` and ```--exclude``` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.


### **More information**
- [Flipside](https://flipsidecrypto.xyz/)
- [Data Studio](https://flipsidecrypto.xyz/edit)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/solana-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}