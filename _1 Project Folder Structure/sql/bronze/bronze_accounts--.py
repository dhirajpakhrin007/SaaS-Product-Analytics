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

# Create Bronze Table (raw structure with no transformations) -- 
con.execute("""
CREATE OR REPLACE TABLE bronze__accounts AS
SELECT 
    account_id,
    account_name,
    country_code,
    city,
    industry,
    employee_band,
    segment,
    created_at,
    trial_start_date,
    trial_end_date,
    account_status,
    acquisition_channel
FROM raw__accounts;            
""")

print("Created bronze_accounts table")

con.close()