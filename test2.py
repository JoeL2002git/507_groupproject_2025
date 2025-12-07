from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv(".env")

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_TABLE = os.getenv('DB_TABLE')

url_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}"
conn = create_engine(url_string)
table = DB_TABLE

print("="*80)
print("CHECKING WHAT METRICS EACH TEAM HAS")
print("="*80)

for team_name in ['Mens Basketball', 'Womens Basketball', "Men's Basketball", "Women's Basketball"]:
    print(f"\n{team_name}:")
    print("-" * 60)
    
    query = f"""
    SELECT 
        metric,
        COUNT(DISTINCT playername) as num_athletes,
        COUNT(*) as num_measurements
    FROM {table}
    WHERE team = '{team_name}'
      AND value IS NOT NULL
    GROUP BY metric
    ORDER BY num_athletes DESC
    """
    
    result = pd.read_sql(query, conn)
    if len(result) > 0:
        print(result.to_string(index=False))
    else:
        print("  No data found")

print("\n" + "="*80)
print("ATHLETES IN EACH TEAM (ANY METRIC):")
print("="*80)

for team_name in ['Mens Basketball', 'Womens Basketball', "Men's Basketball", "Women's Basketball"]:
    query = f"""
    SELECT COUNT(DISTINCT playername) as count
    FROM {table}
    WHERE team = '{team_name}'
      AND value IS NOT NULL
    """
    result = pd.read_sql(query, conn)
    print(f"{team_name}: {result.iloc[0]['count']} athletes")