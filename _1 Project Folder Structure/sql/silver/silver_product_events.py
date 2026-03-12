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
CREATE OR REPLACE TABLE silver__product_events AS
WITH base AS (
    SELECT
        event_id,
        event_name,
        user_id,
        account_id,
        deal_id,
        event_timestamp,
        ingested_at,
        event_date,
        platform,
        device_type,
        app_version,
        UPPER(country_code) AS country_code,
        event_properties,
        is_test_event,
        source_system
    FROM bronze_product_events
    WHERE is_test_event = FALSE
),

enriched AS (
    SELECT 
        *,
        CAST(event_timestamp AS DATE)   AS event_ts_date,
        DATE_TRUNC('month', event_timestamp)    AS event_ts_month,
        
        CASE 
            WHEN event_name ILIKE '%login%' THEN 'AUTH'
            WHEN event_name ILIKE '%pipeline%' OR event_name ILIKE '%deal%' THEN 'PIPELINE'
            WHEN event_name ILIKE '%activity%' OR event_name ILIKE '%call' OR event_name ILIKE '%email%' THEN 'ACTIVITY'
            WHEN event_name ILIKE '%workflow%' OR event_name ILIKE '%automation%' THEN 'AUTOMATION'
            ELSE 'OTHER'
        END AS event_category,
        
        CASE
            WHEN deal_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_deal_context
        
    FROM base
)

SELECT 
    event_id,
    event_name  ,
    event_category,
    
    user_id,
    account_id,
    deal_id,
    has_deal_context,
    
    event_timestamp,
    event_ts_Date,
    event_ts_month,
    
    ingested_at,
    event_date,
    
    platform,
    device_type,
    app_version,
    country_code,
    
    event_properties,
    source_system

FROM enriched;
""")

print("Created silver__product_events")

con.close()

