
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
  );

