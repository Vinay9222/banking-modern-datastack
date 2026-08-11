-- created_at: 2026-08-10T13:08:43.248453700+00:00
-- finished_at: 2026-08-10T13:08:43.352909300+00:00
-- elapsed: 104ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c64bf4-0001-f8ed-000f-60fa0003d296
-- desc: execute adapter call
show terse schemas in database BANKING
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-10T13:08:43.354620500+00:00
-- finished_at: 2026-08-10T13:08:43.473866400+00:00
-- elapsed: 119ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c64bf4-0001-f8e9-000f-60fa0003c2c2
-- desc: execute adapter call
create schema if not exists BANKING.ANALYTICS
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-10T13:08:43.527662200+00:00
-- finished_at: 2026-08-10T13:08:43.658564+00:00
-- elapsed: 130ms
-- outcome: success
-- dialect: snowflake
-- node_id: snapshot.banking_dbt.accounts_snapshot
-- query_id: 01c64bf4-0001-f8e9-000f-60fa0003c2c6
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "BANKING"."ANALYTICS" LIMIT 10000;
-- created_at: 2026-08-10T13:08:43.687159800+00:00
-- finished_at: 2026-08-10T13:08:43.784695300+00:00
-- elapsed: 97ms
-- outcome: success
-- dialect: snowflake
-- node_id: snapshot.banking_dbt.customers_snapshot
-- query_id: 01c64bf4-0001-f8ea-000f-60fa0003e2be
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "BANKING"."ANALYTICS" LIMIT 10000;
-- created_at: 2026-08-10T13:08:43.682222200+00:00
-- finished_at: 2026-08-10T13:08:43.838015300+00:00
-- elapsed: 155ms
-- outcome: success
-- dialect: snowflake
-- node_id: snapshot.banking_dbt.accounts_snapshot
-- query_id: 01c64bf4-0001-f8ea-000f-60fa0003e2ba
-- desc: get_column_schema_from_query adapter call
select * from (
        
    

    select *,
        md5(coalesce(cast(account_id as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
        
  
  coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null)
  as dbt_valid_to
from (
        


SELECT * FROM BANKING.ANALYSTICS.stg_accounts


    ) sbq



    ) as __dbt_sbq
    where false
    limit 0
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "snapshot.banking_dbt.accounts_snapshot", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-10T13:08:43.859685700+00:00
-- finished_at: 2026-08-10T13:08:43.921951700+00:00
-- elapsed: 62ms
-- outcome: success
-- dialect: snowflake
-- node_id: snapshot.banking_dbt.accounts_snapshot
-- query_id: 01c64bf4-0001-f8ed-000f-60fa0003d2a2
-- desc: get_column_schema_from_query adapter call
select * from (
        select to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_snapshot_time
    ) as __dbt_sbq
    where false
    limit 0
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "snapshot.banking_dbt.accounts_snapshot", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-10T13:08:43.795266100+00:00
-- finished_at: 2026-08-10T13:08:44.525629300+00:00
-- elapsed: 730ms
-- outcome: success
-- dialect: snowflake
-- node_id: snapshot.banking_dbt.customers_snapshot
-- query_id: 01c64bf4-0001-f8ed-000f-60fa0003d29e
-- desc: get_column_schema_from_query adapter call
select * from (
        
    

    select *,
        md5(coalesce(cast(customer_id as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
        
  
  coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null)
  as dbt_valid_to
from (
        

SELECT * FROM BANKING.ANALYSTICS.stg_customers

    ) sbq



    ) as __dbt_sbq
    where false
    limit 0
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "snapshot.banking_dbt.customers_snapshot", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-10T13:08:43.928702100+00:00
-- finished_at: 2026-08-10T13:08:44.987620800+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: snapshot.banking_dbt.accounts_snapshot
-- query_id: 01c64bf4-0001-f8ed-000f-60fa0003d2a6
-- desc: execute adapter call
create or replace transient  table BANKING.ANALYTICS.accounts_snapshot
    
    
    
    
    as (
    

    select *,
        md5(coalesce(cast(account_id as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
        
  
  coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null)
  as dbt_valid_to
from (
        


SELECT * FROM BANKING.ANALYSTICS.stg_accounts


    ) sbq



    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "snapshot.banking_dbt.accounts_snapshot", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-10T13:08:44.533631800+00:00
-- finished_at: 2026-08-10T13:08:45.009970400+00:00
-- elapsed: 476ms
-- outcome: success
-- dialect: snowflake
-- node_id: snapshot.banking_dbt.customers_snapshot
-- query_id: 01c64bf4-0001-f8ea-000f-60fa0003e2c6
-- desc: get_column_schema_from_query adapter call
select * from (
        select to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_snapshot_time
    ) as __dbt_sbq
    where false
    limit 0
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "snapshot.banking_dbt.customers_snapshot", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-10T13:08:45.018790200+00:00
-- finished_at: 2026-08-10T13:08:46.569356500+00:00
-- elapsed: 1.6s
-- outcome: success
-- dialect: snowflake
-- node_id: snapshot.banking_dbt.customers_snapshot
-- query_id: 01c64bf4-0001-f8e9-000f-60fa0003c2ce
-- desc: execute adapter call
create or replace transient  table BANKING.ANALYTICS.customers_snapshot
    
    
    
    
    as (
    

    select *,
        md5(coalesce(cast(customer_id as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
        
  
  coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null)
  as dbt_valid_to
from (
        

SELECT * FROM BANKING.ANALYSTICS.stg_customers

    ) sbq



    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "snapshot.banking_dbt.customers_snapshot", "profile_name": "banking_dbt", "target_name": "dev"} */;
