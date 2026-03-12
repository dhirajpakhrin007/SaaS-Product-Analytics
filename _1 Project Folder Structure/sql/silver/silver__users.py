import duckdb
import os

# Connect to duckdb file
con = duckdb.connect(
    os.path.join(
        os.path.dirname(__file__),
        "..", "..",
        "product_analytics.db"
    )
)

con.execute("""
CREATE OR REPLACE TABLE silver__users AS
SELECT 
    user_id,
    account_id,
    
    -- Core descriptors
    full_name,
    LOWER(SPLIT_PART(email, '@', 2)) AS email_domain,
    job_role,
    user_status,
    
    --Dates
    created_At,
    CAST(created_at AS DATE) AS created_date,
    last_seen_at,
    
    -- Tenure & Recency
    CASE
        WHEN CAST(created_at AS DATE) > CURRENT_DATE THEN 0
        ELSE DATE_DIFF('day', CAST(created_at AS DATE), CURRENT_DATE)
    END AS user_tenure_days,
    
    CASE
        WHEN last_seen_at IS NULL THEN NULL
        ELSE DATE_DIFF('day', CAST(last_seen_at AS DATE), CURRENT_DATE)
    END AS days_since_last_seen,
    
    CASE
        WHEN last_seen_at IS NULL THEN 'never seen'
        WHEN DATE_DIFF('day', CAST(last_seen_at AS DATE), CURRENT_DATE) <= 7 THEN '0-7 days'
        WHEN DATE_DIFF('day', CAST(last_seen_at AS DATE), CURRENT_DATE) <= 30 THEN '8-30 days'
        WHEN DATE_DIFF('day', CAST(last_seen_at AS DATE), CURRENT_DATE) <= 90 THEN '31-90 days'
        ELSE '90+ days'
    END AS recency_bucket,
    
    -- Flags
    (user_status = 'active') AS is_active_user,
    is_admin

FROM bronze__users;
""")

print("Created silver__users")

con.close()