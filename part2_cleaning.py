2.1 Missing Data Analysis (Group)
!pip install pymysql sqlalchemy pandas python-dotenv

from sqlalchemy import create_engine
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv('test.env')

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_TABLE = os.getenv('DB_TABLE')

DB_USER

url_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}"

conn = create_engine(url_string)

table = "research_experiment_refactor_test"

## Question 1: Identify which of your selected metrics have the most NULL or zero values
query1_focused = """
SELECT
    metric,
    COUNT(*) as total_records,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_count,
    SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END) as zero_count,
    SUM(CASE WHEN value IS NULL OR value = 0 THEN 1 ELSE 0 END) as null_or_zero_count,
    ROUND(100.0 * SUM(CASE WHEN value IS NULL OR value = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as null_zero_percentage
FROM research_experiment_refactor_test
WHERE metric IN (
    'accel_load_accum',
    'Jump Height(m)',
    'Peak Propulsive Force(N)',
    'distance_total',
    'leftMaxForce',
    'rightMaxForce'
)
GROUP BY metric
ORDER BY null_zero_percentage DESC
"""

df_null_focused = pd.read_sql(query1_focused, conn)

print("="*80)
print("NULL/Zero Analysis - Sorted from HIGHEST to LOWEST percentage:")
print("="*80)
print(df_null_focused.to_string(index=False))

# Print the metric with the MOST NULL/zero values
worst_metric = df_null_focused.iloc[0]
print("\n" + "="*80)
print("METRIC WITH MOST NULL/ZERO VALUES:")
print("="*80)
print(f"Metric: {worst_metric['metric']}")
print(f"NULL Count: {worst_metric['null_count']:.0f}")
print(f"Zero Count: {worst_metric['zero_count']:.0f}")
print(f"Total NULL or Zero: {worst_metric['null_or_zero_count']:.0f}")
print(f"Percentage: {worst_metric['null_zero_percentage']:.2f}%")

NULL/Zero Analysis - Sorted from HIGHEST to LOWEST percentage:
================================================================================
    metric	                   total_records	null_count	zero_count	null_or_zero_count	null_zero_percentage
------------------------------|---------------|------------|-----------|-------------------|---------------------
0	distance_total	          | 40803	      |  0.0	   |  486.0	   | 486.0	           | 1.19
1	rightMaxForce	          | 4275	      |  0.0	   |  11.0	   | 11.0	           | 0.26
2	accel_load_accum	      | 40803	      |  0.0	   |  100.0	   | 100.0	           | 0.25
3	leftMaxForce	          | 4275	      |  0.0	   |  9.0	   | 9.0	           | 0.21
4	Jump Height(m)	          | 32123	      |  0.0	   |  0.0	   | 0.0	           | 0.00
5	Peak Propulsive Force(N)  | 32123	      |  0.0	   |  0.0	   | 0.0	           | 0.00

METRIC WITH MOST NULL/ZERO VALUES:
================================================================================
Metric: distance_total
NULL Count: 0
Zero Count: 486
Total NULL or Zero: 486
Percentage: 1.19%

## Question 2: For each sport/team, calculate what percentage of athletes have at least 5 measurements for your selected metrics
query2_option2 = """
SELECT
    team,
    metric,
    COUNT(DISTINCT playername) as total_athletes,
    SUM(CASE WHEN measurement_count >= 5 THEN 1 ELSE 0 END) as athletes_with_5plus,
    ROUND(100.0 * SUM(CASE WHEN measurement_count >= 5 THEN 1 ELSE 0 END) / COUNT(DISTINCT playername), 2) as percentage_with_5plus
FROM (
    SELECT
        playername,
        team,
        metric,
        COUNT(*) as measurement_count
    FROM research_experiment_refactor_test
    WHERE value IS NOT NULL
      AND metric IN (
          'accel_load_accum',
          'Jump Height(m)',
          'Peak Propulsive Force(N)',
          'distance_total',
          'leftMaxForce',
          'rightMaxForce'
      )
    GROUP BY playername, team, metric
) subquery
GROUP BY team, metric
ORDER BY team, metric
"""

df_coverage_option = pd.read_sql(query2_option2, conn)
print("Option 2: Athletes with ≥5 measurements PER METRIC (by Team):")
df_coverage_option

Athletes with ≥5 measurements PER METRIC (by Team):
    team	            metric	           total_athletes	athletes_with_5plus  percentage_with_5plus
-----------------------|-------------------|----------------|--------------------|----------------------
0	Baseball	       | leftMaxForce	   | 63	            | 14.0	             | 22.22
1	Baseball	       | rightMaxForce	   | 63	            | 14.0	             | 22.22
2	Football	       | accel_load_accum  | 44	            | 43.0	             | 97.73
3	Football	       | distance_total	   | 44	            | 43.0	             | 97.73
4	Football	       | leftMaxForce	   | 115	        | 63.0	             | 54.78
...	...	...	...	...	...
183	Women's Soccer	   | rightMaxForce	   | 41	            | 28.0	             | 68.29
184	Womens Basketball  | accel_load_accum  | 76	            | 63.0	             | 82.89
185	Womens Basketball  | distance_total	   | 76	            | 63.0	             | 82.89
186	Womens Soccer	   | accel_load_accum  | 52	            | 51.0	             | 98.08
187	Womens Soccer	   | distance_total	   | 52	            | 51.0	             | 98.08

2.2 Data Transformation Challenge

import pandas as pd
def transform_player_metrics(df, player_name, metrics):
    """
    Filters a DataFrame for a specific player and a list of metrics,
    then pivots the data to a wide format.

    Args:
        df (pd.DataFrame): The input DataFrame containing player metrics.
        player_name (str): The name of the player to filter for.
        metrics (list): A list of metric names to include.

    Returns:
        pd.DataFrame: A wide-format DataFrame with timestamps as index
                      and selected metrics as columns.
    """
    # Filter the DataFrame for the specified player and metrics
    filtered_df = df[
        (df["playername"] == player_name) &
        (df["metric"].isin(metrics))
    ]

    # Pivot the filtered DataFrame to a wide format
    wide_df = filtered_df.pivot_table(
        index="timestamp",
        columns="metric",
        values="value",
        aggfunc="mean" # Use mean to handle potential duplicates for a timestamp-metric pair
    )
    return wide_df

# Example outputs for 3 athletes from different teams
def print_example_transforms(df):
    selected_metrics = ["jump_height", "Peak Propulsive Force(N)	", "distance_total	", "accel_load_accum", "leftMaxForce","rightMaxForce"]

    players_to_test = [
        "PLAYER_005",
        "PLAYER_014",
        "PLAYER_001"
    ]

    for p in players_to_test:
        print("\n=====================================")
        print(f"WIDE FORMAT OUTPUT FOR {p}")
        print("=====================================")

        transformed = transform_player_metrics(df, p, selected_metrics)
        print(transformed.head())  

# Use the 'response' DataFrame, which contains the data from the database
print_example_transforms(response) 
"""
=====================================
WIDE FORMAT OUTPUT FOR PLAYER_005
=====================================
metric               leftMaxForce  rightMaxForce
timestamp                                       
2024-08-06 14:24:21         351.0         312.75
2024-10-30 17:55:56         462.5         375.75

=====================================
WIDE FORMAT OUTPUT FOR PLAYER_014
=====================================
metric               accel_load_accum  leftMaxForce  rightMaxForce
timestamp                                                         
2023-06-16 12:01:47        607.066123           NaN            NaN
2023-06-20 11:58:40          0.009541           NaN            NaN
2023-06-20 11:59:15        629.562592           NaN            NaN
2023-06-21 13:30:00        359.586334           NaN            NaN
2023-06-22 12:00:00        331.550644           NaN            NaN

=====================================
WIDE FORMAT OUTPUT FOR PLAYER_015
=====================================
metric               leftMaxForce  rightMaxForce
timestamp                                       
2024-09-09 11:35:50        467.75         442.00
2025-03-18 20:04:21        394.50         392.75
2025-09-08 12:25:41        506.25         473.50
"""
