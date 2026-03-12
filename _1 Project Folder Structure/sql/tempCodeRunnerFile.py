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
    SELECT u.user_id,
           u.account_id
    FROM silver__users u
    LEFT JOIN silver__accounts a 
    ON u.account_id = a.account_id
    WHERE a.account_id IS NULL;     
""")

con.close()