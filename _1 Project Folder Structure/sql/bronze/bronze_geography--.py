import duckdb
import os
import pandas as pd

# Connect to duckdb file
con = duckdb.connect(
    os.path.join(
        os.path.dirname(__file__),
        "..", "..",
        "product_analytics.db"
    )
)

# Excel file path
excel_path = os.path.join(
    os.path.dirname(__file__),
    "..", "..",
    "raw__geography--.xlsx"
)

# Load excel file into dataframe
df = pd.read_excel(excel_path, sheet_name=0) 

# registering dataframe as temp view
con.register("raw_geography_df", df)

# Creating bronze table for geography
con.execute("""
CREATE OR REPLACE TABLE bronze__geography AS
SELECT 
    country_code,
    country_name,
    region,
    market,
    currency,
    sales_region
FROM raw_geography_df;            
""")

print("bronze geography created")

con.close()
