## Profile Set Up

#### Use the following within profiles.yml 
----

```yml
solana:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <ACCOUNT>
      role: <ROLE>
      user: <USERNAME>
      password: <PASSWORD>
      region: <REGION>
      database: SOLANA_DEV
      warehouse: <WAREHOUSE>
      schema: SILVER
      threads: 4
      client_session_keep_alive: False
      query_tag: <TAG>
```

### Notes
- `block_id` column in our data models matches what [Solscan](https://solscan.io/) classifies as `Block`.  This value is same as what is referred to as `Slot`.

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

## Applying Model Tags

### Database / Schema level tags

Database and schema tags are applied via the `add_database_or_schema_tags` macro.  These tags are inherited by their downstream objects.  To add/modify tags call the appropriate tag set function within the macro.

```
{{ set_database_tag_value('SOME_DATABASE_TAG_KEY','SOME_DATABASE_TAG_VALUE') }}
{{ set_schema_tag_value('SOME_SCHEMA_TAG_KEY','SOME_SCHEMA_TAG_VALUE') }}
```

### Model tags

To add/update a model's snowflake tags, add/modify the `meta` model property under `config`.  Only table level tags are supported at this time via DBT.

```
{{ config(
    ...,
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'SOME_PURPOSE'
            }
        }
    },
    ...
) }}
```

By default, model tags are pushed to Snowflake on each DBT run. You can disable this by setting the `UPDATE_SNOWFLAKE_TAGS` project variable to `False` during a run.

```
dbt run --var '{"UPDATE_SNOWFLAKE_TAGS":False}' -s models/gold/defi/defi__fact_swaps.sql
```

### Querying for existing tags on a model in snowflake

```
select *
from table(solana.information_schema.tag_references('solana.core.fact_blocks', 'table'));
```