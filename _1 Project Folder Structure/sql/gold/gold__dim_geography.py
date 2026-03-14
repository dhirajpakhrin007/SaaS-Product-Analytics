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
CREATE OR REPLACE TABLE gold__dim_geography AS
SELECT
    country_code    AS CountryCode,
    country_name    AS CountryName,
    region          AS Region,
    market          AS Market,
    sales_region    AS SalesRegion,
    currency        AS Currency
FROM silver__geography
ORDER BY CountryCode;            
""")

print("Created gold__dim_geography")

con.close()