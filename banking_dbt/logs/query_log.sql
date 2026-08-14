-- created_at: 2026-08-13T06:56:19.548251700+00:00
-- finished_at: 2026-08-13T06:56:19.771881500+00:00
-- elapsed: 223ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c65b60-0001-fa45-000f-60fa00090006
-- desc: execute adapter call
show terse schemas in database BANKING
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:19.958911900+00:00
-- finished_at: 2026-08-13T06:56:20.389387100+00:00
-- elapsed: 430ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.stg_accounts
-- query_id: 01c65b60-0001-fa55-000f-60fa0008e556
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "BANKING"."ANALYSTICS" LIMIT 10000;
-- created_at: 2026-08-13T06:56:20.291813200+00:00
-- finished_at: 2026-08-13T06:56:20.494615600+00:00
-- elapsed: 202ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.stg_customers
-- query_id: 01c65b60-0001-fa45-000f-60fa0009000e
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "BANKING"."ANALYSTICS" LIMIT 10000;
-- created_at: 2026-08-13T06:56:20.406043+00:00
-- finished_at: 2026-08-13T06:56:20.674572+00:00
-- elapsed: 268ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.stg_accounts
-- query_id: 01c65b60-0001-fa55-000f-60fa0008e55e
-- desc: execute adapter call
create or replace   view BANKING.ANALYSTICS.stg_accounts
  
  
  
  
  as (
    

with ranked as (
    select
        v:id::string            as account_id,
        v:customer_id::string   as customer_id,
        v:account_type::string  as account_type,
        v:balance::float        as balance,
        v:currency::string      as currency,
        v:created_at::timestamp as created_at,
        current_timestamp       as load_timestamp,
        row_number() over (
            partition by v:id::string
            order by v:created_at desc
        ) as rn
    from BANKING.RAW.accounts
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
from ranked
where rn = 1
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.stg_accounts", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:20.745063100+00:00
-- finished_at: 2026-08-13T06:56:20.881873900+00:00
-- elapsed: 136ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.stg_transactions
-- query_id: 01c65b60-0001-fa45-000f-60fa00090016
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "BANKING"."ANALYSTICS" LIMIT 10000;
-- created_at: 2026-08-13T06:56:20.499415400+00:00
-- finished_at: 2026-08-13T06:56:20.931191700+00:00
-- elapsed: 431ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.stg_customers
-- query_id: 01c65b60-0001-f98e-000f-60fa0008f4ba
-- desc: execute adapter call
create or replace   view BANKING.ANALYSTICS.stg_customers
  
  
  
  
  as (
    

with parsed as (
    select
        v:id::string as customer_id,
        v:first_name::string as first_name,
        v:last_name::string as last_name,
        v:email::string as email,
        v:created_at::timestamp as created_time,
        v:updated_at::timestamp as updated_time
    from
        BANKING.RAW.customers
),

ranked as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        created_time,
        updated_time,
        rank() over(partition by customer_id order by created_time) as rnk
    from
        parsed
)

select 
    customer_id,
    first_name,
    last_name,
    email,
    created_time,
    updated_time
from ranked
where rnk = 1
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.stg_customers", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:20.889909200+00:00
-- finished_at: 2026-08-13T06:56:21.248854800+00:00
-- elapsed: 358ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.stg_transactions
-- query_id: 01c65b60-0001-f98e-000f-60fa0008f4c2
-- desc: execute adapter call
create or replace   view BANKING.ANALYSTICS.stg_transactions
  
  
  
  
  as (
    

SELECT
    v:id::string                 AS transaction_id,
    v:account_id::string         AS account_id,
    v:amount::float              AS amount,
    v:txn_type::string           AS transaction_type,
    v:related_account_id::string AS related_account_id,
    v:status::string             AS status,
    v:created_at::timestamp      AS transaction_time,
    CURRENT_TIMESTAMP            AS load_timestamp
FROM BANKING.RAW.transactions
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.stg_transactions", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:21.274843900+00:00
-- finished_at: 2026-08-13T06:56:21.689111100+00:00
-- elapsed: 414ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.fact_transactions
-- query_id: 01c65b60-0001-fa45-000f-60fa0009001e
-- desc: execute adapter call
create or replace  temporary view BANKING.ANALYSTICS.fact_transactions__dbt_tmp
  
  
  
  
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
-- created_at: 2026-08-13T06:56:21.693707700+00:00
-- finished_at: 2026-08-13T06:56:21.875872100+00:00
-- elapsed: 182ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.fact_transactions
-- query_id: 01c65b60-0001-fa45-000f-60fa00090022
-- desc: execute adapter call
describe table BANKING.ANALYSTICS.fact_transactions__dbt_tmp
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.fact_transactions", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:21.883827400+00:00
-- finished_at: 2026-08-13T06:56:21.975771800+00:00
-- elapsed: 91ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.fact_transactions
-- query_id: 01c65b60-0001-f98e-000f-60fa0008f4c6
-- desc: execute adapter call
describe table BANKING.ANALYSTICS.fact_transactions
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.fact_transactions", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:21.979698900+00:00
-- finished_at: 2026-08-13T06:56:22.093742300+00:00
-- elapsed: 114ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.fact_transactions
-- query_id: 01c65b60-0001-fa45-000f-60fa00090026
-- desc: execute adapter call
describe table "BANKING"."ANALYSTICS"."FACT_TRANSACTIONS"
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.fact_transactions", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:22.108380700+00:00
-- finished_at: 2026-08-13T06:56:22.234030100+00:00
-- elapsed: 125ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.fact_transactions
-- query_id: 01c65b60-0001-fa55-000f-60fa0008e566
-- desc: execute adapter call
-- back compat for old kwarg name
  
  begin
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.fact_transactions", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:20.704163400+00:00
-- finished_at: 2026-08-13T06:56:22.903936300+00:00
-- elapsed: 2.2s
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.dim_accounts
-- query_id: 01c65b60-0001-f98e-000f-60fa0008f4be
-- desc: execute adapter call
create or replace transient  table BANKING.ANALYSTICS.dim_accounts
    
    
    
    
    as (

WITH source_data AS (
    SELECT
        account_id,
        customer_id,
        account_type,
        balance,
        currency,
        created_at,
        dbt_valid_from   AS effective_from,
        dbt_valid_to     AS effective_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM BANKING.ANALYTICS.accounts_snapshot
)

SELECT * FROM source_data
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.dim_accounts", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:20.941951400+00:00
-- finished_at: 2026-08-13T06:56:22.954019300+00:00
-- elapsed: 2.0s
-- outcome: success
-- dialect: snowflake
-- node_id: model.banking_dbt.dim_customers
-- query_id: 01c65b60-0001-fa55-000f-60fa0008e562
-- desc: execute adapter call
create or replace transient  table BANKING.ANALYSTICS.dim_customers
    
    
    
    
    as (

WITH latest AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        created_time,
        dbt_valid_from   AS effective_from,
        dbt_valid_to     AS effective_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM BANKING.ANALYTICS.customers_snapshot
)

SELECT * FROM latest
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.dim_customers", "profile_name": "banking_dbt", "target_name": "dev"} */;
-- created_at: 2026-08-13T06:56:22.234720+00:00
-- finished_at: 2026-08-13T06:56:26.118937300+00:00
-- elapsed: 3.9s
-- outcome: error
-- error vendor code: 100090
-- error message: Internal: [Snowflake] 100090 (42P18): Duplicate row detected during DML action
Row Values: ["3", "7", "5", 805.19, NULL, "COMPLETED", "DEPOSIT", 1784697774203408000, 1786538589098000000]
-- dialect: snowflake
-- node_id: model.banking_dbt.fact_transactions
-- query_id: not available
-- desc: execute adapter call

    
        
            
            
            
            
        
    

    

    merge into BANKING.ANALYSTICS.fact_transactions as DBT_INTERNAL_DEST
        using BANKING.ANALYSTICS.fact_transactions__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.transaction_id = DBT_INTERNAL_DEST.transaction_id))

    
    when matched then update set
        "TRANSACTION_ID" = DBT_INTERNAL_SOURCE."TRANSACTION_ID","ACCOUNT_ID" = DBT_INTERNAL_SOURCE."ACCOUNT_ID","CUSTOMER_ID" = DBT_INTERNAL_SOURCE."CUSTOMER_ID","AMOUNT" = DBT_INTERNAL_SOURCE."AMOUNT","RELATED_ACCOUNT_ID" = DBT_INTERNAL_SOURCE."RELATED_ACCOUNT_ID","STATUS" = DBT_INTERNAL_SOURCE."STATUS","TRANSACTION_TYPE" = DBT_INTERNAL_SOURCE."TRANSACTION_TYPE","TRANSACTION_TIME" = DBT_INTERNAL_SOURCE."TRANSACTION_TIME","LOAD_TIMESTAMP" = DBT_INTERNAL_SOURCE."LOAD_TIMESTAMP"
    

    when not matched then insert
        ("TRANSACTION_ID", "ACCOUNT_ID", "CUSTOMER_ID", "AMOUNT", "RELATED_ACCOUNT_ID", "STATUS", "TRANSACTION_TYPE", "TRANSACTION_TIME", "LOAD_TIMESTAMP")
    values
        ("TRANSACTION_ID", "ACCOUNT_ID", "CUSTOMER_ID", "AMOUNT", "RELATED_ACCOUNT_ID", "STATUS", "TRANSACTION_TYPE", "TRANSACTION_TIME", "LOAD_TIMESTAMP")


/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.banking_dbt.fact_transactions", "profile_name": "banking_dbt", "target_name": "dev"} */;
