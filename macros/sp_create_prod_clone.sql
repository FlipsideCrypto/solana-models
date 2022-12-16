{% macro sp_create_prod_clone(target_schema) -%}

create or replace procedure {{ target_schema }}.create_prod_clone(source_db_name string, destination_db_name string, role_name string)
returns boolean 
language javascript
execute as caller
as
$$
    snowflake.execute({sqlText: `BEGIN TRANSACTION;`});
    try {
        snowflake.execute({sqlText: `CREATE OR REPLACE DATABASE ${DESTINATION_DB_NAME} CLONE ${SOURCE_DB_NAME}`});
        snowflake.execute({sqlText: `DROP SCHEMA IF EXISTS ${DESTINATION_DB_NAME}._INTERNAL`}); /* this only needs to be in prod */
        snowflake.execute({sqlText: `GRANT USAGE ON DATABASE ${DESTINATION_DB_NAME} TO AWS_LAMBDA_SOLANA_API`}); 

        snowflake.execute({sqlText: `GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON ALL FUNCTIONS IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON ALL PROCEDURES IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON ALL VIEWS IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON ALL STAGES IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON ALL TABLES IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON FUTURE FUNCTIONS IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME};`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON FUTURE PROCEDURES IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME};`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON FUTURE VIEWS IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME};`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON FUTURE STAGES IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME};`}); 
        snowflake.execute({sqlText: `GRANT OWNERSHIP ON FUTURE TABLES IN DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME};`}); 
        snowflake.execute({sqlText: `GRANT USAGE ON ALL STAGES IN DATABASE ${DESTINATION_DB_NAME} TO ROLE AWS_LAMBDA_SOLANA_API;`}); 

        snowflake.execute({sqlText: `GRANT OWNERSHIP ON DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`})

        snowflake.execute({sqlText: `COMMIT;`});
    } catch (err) {
        snowflake.execute({sqlText: `ROLLBACK;`});
        throw(err);
    }

    return true
$$

{%- endmacro %}