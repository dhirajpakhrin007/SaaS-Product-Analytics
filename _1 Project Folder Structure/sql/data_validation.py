import duckdb
import os

# Connect to duckdb file
con = duckdb.connect(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "product_analytics.db"
    )
)

con.execute("""
    SELECT 
        session_id,
        user_id,
        session_start,
        session_end,
        session_duration_seconds_clean
    FROM silver__user_sessions
    WHERE session_duration_seconds_clean <= 0;   
""")

con.close()