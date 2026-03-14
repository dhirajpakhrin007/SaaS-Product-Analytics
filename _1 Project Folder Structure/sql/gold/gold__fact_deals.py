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
CREATE OR REPLACE TABLE gold__fact_deals AS 
SELECT
    deal_id             AS DealId,
    account_id          AS AccountId,
    owner_user_id       AS OwnerUserId,
    
    pipeline_id         AS PipelineId,
    current_stage_id    AS StagedId,
    
    status              AS Status,
    
    created_at          AS CreatedAt,
    created_date        AS CreatedDate,
    
    closed_at           AS ClosedAt,
    closed_date         AS ClosedDate,
    
    last_stage_changed_at   AS LastStageChangedAt,
    last_stage_changed_date AS LastStageChangedDate,
    
    amount              AS DealAmount,
    currency           AS Currency,
    country_code        AS CountryCode,
    
    source_system       AS SourceSystem,
    
    is_closed           AS IsClosed,
    is_won              AS IsWon,
    
    deal_cycle_days     AS DealCycleDays,
    deal_age_days       AS DealAgeDays
FROM silver__deals
ORDER BY DealId;            
""")

print("Created gold__fact_deals")

con.close()