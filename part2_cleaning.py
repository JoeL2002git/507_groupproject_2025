2.1 Missing Data Analysis (Group)

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
    'leftMaxForce/rightMaxForce'
)
GROUP BY metric
ORDER BY null_zero_percentage DESC
"""

df_null_focused = pd.read_sql(query1_focused, conn)
print("NULL/Zero Analysis for Your 5 Metrics:")
df_null_focused

    metric	                    total_records	null_count	zero_count	null_or_zero_count	null_zero_percentage
0	distance_total	            40803	        0.0	        486.0	    486.0	            1.19
1	accel_load_accum	        40803	        0.0	        100.0	    100.0	            0.25
2	Jump Height(m)	            32123	        0.0	        0.0	        0.0	                0.00
3	Peak Propulsive Force(N)	32123	        0.0	        0.0	        0.0	                0.00



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
