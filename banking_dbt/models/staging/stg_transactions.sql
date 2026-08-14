{# {{ config(materialized='view') }}

SELECT
    v:id::string                 AS transaction_id,
    v:account_id::string         AS account_id,
    v:amount::float              AS amount,
    v:txn_type::string           AS transaction_type,
    v:related_account_id::string AS related_account_id,
    v:status::string             AS status,
    v:created_at::timestamp      AS transaction_time,
    CURRENT_TIMESTAMP            AS load_timestamp
FROM {{ source('raw', 'transactions') }} #}

 {{ config(materialized='view') }}
    
    WITH parsed AS (
        SELECT
            v:id::string                 AS transaction_id,
            v:account_id::string         AS account_id,
            v:amount::float              AS amount,
            v:txn_type::string           AS transaction_type,
            v:related_account_id::string AS related_account_id,
            v:status::string             AS status,
            v:created_at::timestamp      AS transaction_time,
            CURRENT_TIMESTAMP            AS load_timestamp
        FROM {{ source('raw', 'transactions') }}
    ),
    
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC, load_timestamp DESC
            ) AS rn
        FROM parsed
    )
    
    SELECT
        transaction_id,
        account_id,
        amount,
        transaction_type,
        related_account_id,
        status,
        transaction_time,
        load_timestamp
    FROM ranked
    WHERE rn = 1