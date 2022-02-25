{% macro sp_create_prod_clone(target_schema) -%}

create or replace procedure {{ target_schema }}.create_prod_clone(source_db_name string, destination_db_name string, role_name string)
returns boolean 
language javascript
execute as caller
as
$$
    snowflake.execute({sqlText: `BEGIN TRANSACTION;`});
    try {
        snowflake.execute({sqlText: `DROP DATABASE IF EXISTS ${DESTINATION_DB_NAME}`});
        snowflake.execute({sqlText: `CREATE DATABASE ${DESTINATION_DB_NAME} CLONE ${SOURCE_DB_NAME}`});

        var existing_schemas = snowflake.execute({sqlText: `SELECT table_schema
            FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.TABLE_PRIVILEGES
            WHERE grantor IS NOT NULL
            GROUP BY 1;`});

        while (existing_schemas.next()) {
            var schema = existing_schemas.getColumnValue(1)
            snowflake.execute({sqlText: `GRANT OWNERSHIP ON SCHEMA ${DESTINATION_DB_NAME}.${schema} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`});
        }

        var existing_tables = snowflake.execute({sqlText: `SELECT table_schema, table_name
            FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.TABLE_PRIVILEGES
            WHERE grantor IS NOT NULL
            GROUP BY 1,2;`});

        while (existing_tables.next()) {
            var schema = existing_tables.getColumnValue(1)
            var table_name = existing_tables.getColumnValue(2)
            snowflake.execute({sqlText: `GRANT OWNERSHIP ON TABLE ${DESTINATION_DB_NAME}.${schema}.${table_name} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;`});
        }

        snowflake.execute({sqlText: `GRANT OWNERSHIP ON DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME};`})
        snowflake.execute({sqlText: `COMMIT;`});
    } catch (err) {
        snowflake.execute({sqlText: `ROLLBACK;`});
        throw(err);
    }
    
    return true
$$

{%- endmacro %}