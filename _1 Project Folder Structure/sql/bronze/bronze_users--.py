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

# Creating Bronze table
con.execute(""" 
CREATE OR REPLACE TABLE bronze__users AS
SELECT
    user_id,
    account_id,
    full_name,
    job_role,
    user_status,
    created_at,
    last_seen_at,
    timezone,
    locale,
    is_admin
FROM raw__users;    
""")

print("Created bronze__users table")

con.close()