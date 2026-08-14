-- created_at: 2026-08-14T12:49:45.059816300+00:00
-- finished_at: 2026-08-14T12:49:45.949727+00:00
-- elapsed: 889ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c66261-0001-f98e-000f-60fa000a212a
-- desc: list_relations_in_parallel
SHOW OBJECTS IN SCHEMA "BANKING"."ANALYSTICS" LIMIT 10000;
-- created_at: 2026-08-14T12:49:46.208779200+00:00
-- finished_at: 2026-08-14T12:49:46.531905700+00:00
-- elapsed: 323ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c66261-0001-f98e-000f-60fa000a2132
-- desc: execute adapter call
show terse schemas in database BANKING
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-14T12:49:46.726415700+00:00
-- finished_at: 2026-08-14T12:49:48.291883+00:00
-- elapsed: 1.6s
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.fact_transactions
-- query_id: 01c66261-0001-fa45-000f-60fa000a40e6
-- desc: execute adapter call
create or replace transient  table BANKING.ANALYSTICS.fact_transactions
    
    
    
    
    as (


  
    
    SELECT
        t.transaction_id,
        t.account_id,
        a.customer_id,
        t.amount,
        t.related_account_id,
        t.status,
        t.transaction_type,
        t.transaction_time,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM BANKING.ANALYSTICS.stg_transactions t
    LEFT JOIN BANKING.ANALYSTICS.stg_accounts a
        ON t.account_id = a.account_id
    
    
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.fact_transactions", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-14T12:49:48.295394300+00:00
-- finished_at: 2026-08-14T12:49:48.381751+00:00
-- elapsed: 86ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.fact_transactions
-- query_id: 01c66261-0001-fa45-000f-60fa000a40ea
-- desc: execute adapter call
drop view if exists BANKING.ANALYSTICS.fact_transactions__dbt_tmp cascade
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.fact_transactions", "profile_name": "banking_dbt", "target_name": "dev"} */;
