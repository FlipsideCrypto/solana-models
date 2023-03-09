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

**Dimension Tables:**
- [dim_labels](#!/model/model.solana_models.core__dim_labels)
- [dim_tokens](#!/model/model.solana_models.core__dim_tokens)
- [dim_nft_metadata](#!/model/model.solana_models.core__nft_metadata)

**Fact Tables:**
- [fact_blocks](#!/model/model.solana_models.core__fact_blocks)
- [fact_events](#!/model/model.solana_models.core__fact_events)
- [fact_gauges_creates](#!/model/model.solana_models.core__fact_events)
- [fact_gauges_votes](#!/model/model.solana_models.core__fact_gauges_votes)
- [fact_gov_actions](#!/model/model.solana_models.core__fact_gov_actions)
- [fact_liquidity_pool_actions](#!/model/model.solana_models.core__fact_liquidity_pool_actions)
- [fact_nft_mints](#!/model/model.solana_models.core__fact_nft_mints)
- [fact_nft_sales](#!/model/model.solana_models.core__fact_nft_sales)
- [fact_proposal_creation](#!/model/model.solana_models.core__fact_proposal_creation)
- [fact_proposal_votes](#!/model/model.solana_models.core__fact_proposal_votes)
- [fact_stake_pool_actions](#!/model/model.solana_models.core__fact_stake_pool_actions)
- [fact_staking_lp_actions](#!/model/model.solana_models.core__fact_staking_lp_actions)
- [fact_swaps](#!/model/model.solana_models.core__fact_swaps)
- [fact_token_prices_hourly](#!/model/model.solana_models.core__fact_token_prices_hourly)
- [fact_transactions](#!/model/model.solana_models.core__fact_transactions)
- [fact_transfers](#!/model/model.solana_models.core__fact_transfers)
- [fact_votes_agg_block](#!/model/model.solana_models.core__fact_votes_agg_block)

**Convenience Tables:**
- [ez_signers](#!/model/model.solana_models.core__ez_signers)
- [ez_staking_lp_actions](#!/model/model.solana_models.core__ez_staking_lp_actions)
- [ez_token_prices_hourly](#!/model/model.solana_models.core__ez_token_prices_hourly)

## **Data Model Overview**

The Solana models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and core.**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Core: Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

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
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/solana-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}