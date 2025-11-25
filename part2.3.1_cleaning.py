"""
part2.3_cleaning.py

Calculates team means for 6 key metrics and adds percent-difference columns
for each athlete's measurements relative to their team's average.

Metrics:
- accel_load_accum (Accumulated Acceleration Load)
- Jump Height(m)
- Peak Propulsive Force(N)
- distance_total (Total Distance)
- leftMaxForce
- rightMaxForce
"""

from sqlalchemy import create_engine
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(env_path)

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_TABLE = os.getenv('DB_TABLE', 'research_experiment_refactor_test')

if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
    raise SystemExit("Missing DB credentials in .env")

url_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}"
conn = create_engine(url_string)

table = DB_TABLE

# Define the 6 metrics
METRICS = [
    'accel_load_accum',
    'Jump Height(m)',
    'Peak Propulsive Force(N)',
    'distance_total',
    'leftMaxForce',
    'rightMaxForce'
]

# ============================================================================
# Step 1: Fetch all data for these metrics (long format)
# ============================================================================
metrics_str = "','".join(METRICS)
query_fetch = f"""
SELECT 
    playername,
    team,
    metric,
    value,
    timestamp
FROM {table}
WHERE metric IN ('{metrics_str}')
  AND value IS NOT NULL
ORDER BY team, playername, metric, timestamp
"""

print("Fetching data for the 6 metrics...")
df_all = pd.read_sql(query_fetch, conn)
print(f"Fetched {len(df_all)} records for {len(df_all['playername'].unique())} athletes")
print(f"Metrics found: {df_all['metric'].unique().tolist()}")

# ============================================================================
# Step 2: Calculate team means (per metric)
# ============================================================================
print("\n" + "="*80)
print("TEAM MEANS FOR EACH METRIC")
print("="*80)

team_means = df_all.groupby(['team', 'metric'])['value'].mean().reset_index()
team_means.columns = ['team', 'metric', 'team_mean']

# Pivot to see metrics as columns
team_means_pivot = team_means.pivot(index='team', columns='metric', values='team_mean')
print(team_means_pivot.to_string())

# ============================================================================
# Step 3: Add percent difference for each measurement
# ============================================================================
print("\n" + "="*80)
print("CALCULATING PERCENT DIFFERENCE FOR EACH ATHLETE MEASUREMENT")
print("="*80)

# Merge team means back onto the original data
df_with_means = df_all.merge(team_means, on=['team', 'metric'], how='left')

# Calculate percent difference: (value - team_mean) / team_mean * 100
df_with_means['pct_diff_from_team'] = (
    (df_with_means['value'] - df_with_means['team_mean']) / df_with_means['team_mean'] * 100
)

print("\nSample of data with percent differences (first 20 rows):")
print(df_with_means[['playername', 'team', 'metric', 'value', 'team_mean', 'pct_diff_from_team']].head(20).to_string(index=False))

# ============================================================================
# Step 4: Summary statistics
# ============================================================================
print("\n" + "="*80)
print("SUMMARY: PERCENT DIFFERENCE STATISTICS BY METRIC")
print("="*80)

summary = df_with_means.groupby('metric').agg({
    'pct_diff_from_team': ['min', 'max', 'mean', 'std']
}).round(2)
summary.columns = ['Min %', 'Max %', 'Mean %', 'Std Dev %']
print(summary.to_string())
