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
CREATE OR REPLACE TABLE gold__fact_product_events AS
SELECT
    event_id            AS EventId,
    
    account_id          AS AccountId,
    user_id             AS UserId,
    deal_id             AS DealId,
    
    event_name          AS EventName,
    event_category      AS EventCategory,
    has_deal_context    AS HasDealContext,
    
    event_timestamp     AS EventTimestamp,
    event_ts_date       AS EventDate,
    event_ts_month      AS EventMonth,
    
    platform            AS Platform,
    device_type         AS DeviceType,
    app_version         AS AppVersion,
    country_code        AS AppVersion,
    country_code        AS CountryCode,
    
    source_system       AS SourceSystem
FROM silver__product_events
ORDER BY EventId;            
""")

print("Created gold__fact_product_events")

con.close()