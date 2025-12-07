""""
Performance Monitoring Flag System - Basketball only, Men's and Women's team are flag separately
Flag Formulas: Asymmetry: ((strong - weak) / strong) * 100%
Acceleration Load: value > 90th percentile of all players within the same team
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

print("="*80)
print("PART 4.1: PERFORMANCE MONITORING FLAG SYSTEM - BASKETBALL ONLY")
print("\nAnalyzing all basketball team variations")
print("\nFlagging Criteria:")
print("  1. Bilateral Asymmetry: ((strong - weak) / strong) * 100% > 10%")
print("  2. Acceleration Load: value > 90th percentile by GENDER")

# Load team list
basketball_teams = [
    'Mens Basketball',
    'Womens Basketball',
    "Men's Basketball",
    "Women's Basketball"
]

# Load acceleration load data
query_accel = f"""
SELECT 
    playername,
    team,
    metric,
    value,
    timestamp
FROM {table}
WHERE metric = 'accel_load_accum'
  AND value IS NOT NULL
  AND team IN ('Mens Basketball', 'Womens Basketball', "Men's Basketball", "Women's Basketball")
ORDER BY playername, timestamp
"""

df_accel = pd.read_sql(query_accel, conn)
df_accel['timestamp'] = pd.to_datetime(df_accel['timestamp'])

# Load left/right max force data
query_bilateral = f"""
SELECT 
    playername,
    team,
    metric,
    value,
    timestamp
FROM {table}
WHERE metric IN ('leftMaxForce', 'rightMaxForce')
  AND value IS NOT NULL
  AND team IN ('Mens Basketball', 'Womens Basketball', "Men's Basketball", "Women's Basketball")
ORDER BY playername, timestamp
"""

df_bilateral_raw = pd.read_sql(query_bilateral, conn)
df_bilateral_raw['timestamp'] = pd.to_datetime(df_bilateral_raw['timestamp'])

# Create gender column
df_accel['gender'] = df_accel['team'].apply(lambda x: 'Male' if 'Men' in x else 'Female')

print(f"\nLoaded {len(df_accel)} accel_load measurements")
print(f"Loaded {len(df_bilateral_raw)} bilateral force measurements")

print(f"\nBreakdown by team (accel load):")
total_team_count = 0
for team in basketball_teams:
    count = len(df_accel[df_accel['team']==team]['playername'].unique())
    if count > 0:
        print(f"  {team}: {count} athletes")
        total_team_count += count

print(f"\nTotal athlete-team combinations: {total_team_count}")
print(f"Unique athletes across all teams: {df_accel['playername'].nunique()}")

print(f"\nBreakdown by gender:")
print(f"  Male: {len(df_accel[df_accel['gender']=='Male'])} measurements")
print(f"  Female: {len(df_accel[df_accel['gender']=='Female'])} measurements")

print("\n" + "="*80)
print("FLAG 1: BILATERAL ASYMMETRY >10%")
print("Formula: ((strong - weak) / strong) * 100%")

if len(df_bilateral_raw) > 0:
    # Get left and right force measurements
    df_left = df_bilateral_raw[df_bilateral_raw['metric'] == 'leftMaxForce'].copy()
    df_right = df_bilateral_raw[df_bilateral_raw['metric'] == 'rightMaxForce'].copy()
    
    # Merge on playername and timestamp
    df_bilateral = pd.merge(
        df_left[['playername', 'team', 'timestamp', 'value']],
        df_right[['playername', 'timestamp', 'value']],
        on=['playername', 'timestamp'],
        suffixes=('_left', '_right')
    )
    
    # Calculate asymmetry using correct formula: ((strong - weak) / strong) * 100
    df_bilateral['strong'] = df_bilateral[['value_left', 'value_right']].max(axis=1)
    df_bilateral['weak'] = df_bilateral[['value_left', 'value_right']].min(axis=1)
    df_bilateral['asymmetry_pct'] = ((df_bilateral['strong'] - df_bilateral['weak']) / df_bilateral['strong']) * 100
    
    # Determine which side is stronger
    df_bilateral['stronger_side'] = df_bilateral.apply(
        lambda row: 'Left' if row['value_left'] > row['value_right'] else 'Right', axis=1
    )
    
    # Flag if asymmetry > 10%
    df_bilateral['flagged'] = df_bilateral['asymmetry_pct'] > 10
    
    # Get most recent test for each athlete
    df_bilateral_latest = df_bilateral.sort_values('timestamp').groupby('playername').tail(1)
    flagged_asymmetry = df_bilateral_latest[df_bilateral_latest['flagged']].copy()
    
    print(f"\nFound {len(flagged_asymmetry)} athletes with >10% bilateral asymmetry")
    print(f"Out of {len(df_bilateral_latest)} athletes tested with bilateral force metrics\n")
    
    if len(flagged_asymmetry) > 0:
        print("Top 10 cases (highest asymmetry):")
        top_cases = flagged_asymmetry.nlargest(10, 'asymmetry_pct')[[
            'playername', 'team', 'value_left', 'value_right', 'stronger_side', 'asymmetry_pct', 'timestamp'
        ]].copy()
        top_cases.columns = ['playername', 'team', 'left_force', 'right_force', 'stronger_side', 'asymmetry_%', 'last_test']
        print(top_cases.to_string(index=False))
    
    flagged_asymmetry['flag_reason'] = 'Bilateral asymmetry >10%'
    flagged_asymmetry['metric_name'] = 'leftMaxForce vs rightMaxForce'
    flagged_asymmetry['flag_value'] = flagged_asymmetry['asymmetry_pct'].round(2)
    flagged_asymmetry['last_test'] = flagged_asymmetry['timestamp']
else:
    print("\nNo bilateral force data available for basketball athletes.")
    flagged_asymmetry = pd.DataFrame()

print("\n" + "="*80)
print("ACCELERATION LOAD ACCUMULATION >90th PERCENTILE (BY GENDER)")

# Calculate 90th percentile by gender
gender_percentiles = df_accel.groupby('gender')['value'].quantile(0.90).reset_index()
gender_percentiles.columns = ['gender', 'percentile_90']

print(f"\nTotal accel_load_accum measurements: {len(df_accel)}")
print(f"\n90th percentile thresholds by gender:")
print(gender_percentiles.to_string(index=False))

# Merge gender percentiles back to data
df_accel = df_accel.merge(gender_percentiles, on='gender', how='left')

# Flag values above gender's 90th percentile
df_accel['flagged'] = df_accel['value'] > df_accel['percentile_90']

# Get most recent test for each athlete
df_accel_latest = df_accel.sort_values('timestamp').groupby('playername').tail(1)
flagged_accel = df_accel_latest[df_accel_latest['flagged']].copy()

print(f"\nFound {len(flagged_accel)} athletes with recent accel_load_accum >90th percentile of their gender")
print(f"Out of {len(df_accel_latest)} athletes tested")

# Breakdown by gender
print("\nFlags by gender:")
for gender in ['Male', 'Female']:
    gender_flags = len(flagged_accel[flagged_accel['gender'] == gender])
    gender_total = len(df_accel_latest[df_accel_latest['gender'] == gender])
    if gender_total > 0:
        print(f"  {gender}: {gender_flags} / {gender_total} ({gender_flags/gender_total*100:.1f}%)")

if len(flagged_accel) > 0:
    print("\n" + "="*80)
    print("TOP 10 CASES (highest acceleration load relative to gender):")
    print("="*80)
    top_cases = flagged_accel.nlargest(10, 'value')[[
        'playername', 'team', 'gender', 'value', 'percentile_90', 'timestamp'
    ]].copy()
    top_cases['pct_above_threshold'] = ((top_cases['value'] - top_cases['percentile_90']) / 
                                        top_cases['percentile_90'] * 100).round(1)
    top_cases.columns = ['playername', 'team', 'gender', 'accel_load', 'gender_90th', 'last_test', '%_above_threshold']
    print(top_cases.to_string(index=False))

flagged_accel['flag_reason'] = 'Accel load >90th percentile (gender)'
flagged_accel['metric_name'] = 'accel_load_accum'
flagged_accel['flag_value'] = flagged_accel['value'].round(2)
flagged_accel['last_test'] = flagged_accel['timestamp']
