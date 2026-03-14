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
CREATE OR REPLACE TABLE gold__fact_product_events_aggregated AS
SELECT
    event_ts_date       AS EventDate,
    event_name          AS EventName,
    event_category      AS EventCategory,
    account_id          AS AccountId,
    
    COUNT(*)            AS EventCount,
    COUNT(DISTINCT user_id) AS UniqueUsers
FROM silver__product_events
GROUP BY
    event_ts_date,
    event_name,
    event_category,
    account_id
ORDER BY
    EventDate,
    EventName,
    AccountId      
""")

print("Created gold__fact_product_events_aggregated")

con.close()