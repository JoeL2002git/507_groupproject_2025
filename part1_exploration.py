from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

#Find the exact path to the .env file and load the environment variables
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(env_path)

# Retrieve database credentials from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')  
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_TABLE = os.getenv('DB_TABLE')

# Create the database connection
url_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}"
conn = create_engine(url_string)

# Test the connection by naming the table and querying a sample of data
table = "research_experiment_refactor_test"
sample = pd.read_sql(f"select * from {table} limit 5", conn)
print(sample)

#Q1: Function to find the number of unique athletes in the database
def unique_athletes(conn):
  query = f"""
  select count(distinct playername) as unique_athletes
  from {table};
  """
  df = pd.read_sql(query, conn)
  return int(df.loc[0, "unique_athletes"])
count = unique_athletes(conn)
print(f"There are {count} unique athletes in the database.")

#Q2. Function to find the number of different sports/teams in the database
def unique_teams(conn):
  query = f"""
  select count(distinct team) as unique_teams
  from {table};
  """
  df = pd.read_sql(query,conn)
  return int(df.loc[0, "unique_teams"])
count = unique_teams(conn)
print(f"There are {count} unique teams in the database.")

#Q3. Function to find the date range of the data in the database
def data_range(conn):
  query = f"""
  select min(timestamp) as earliest,
  max(timestamp) as latest
  from {table};
  """
  df = pd.read_sql(query, conn)
  return df.loc[0, "earliest"], df.loc[0, "latest"]
min, max = data_range(conn)
print(f"The date range of available data is between {min} and {max}")

#Q4. Function to find which data source has the most records in the database
def most_records(conn):
  query = f"""
  select data_source,
  count(*) as records
  from {table}
  group by data_source
  order by records desc
  """
  df = pd.read_sql(query, conn)
  return df
records = most_records(conn)
print(f"The following are the sources with their amount of corresponding records {records}")

#Q5. Function to find the number of athletes with missing or invalid names
def missing_names(conn):
  query = f"""
  select count(*) as missing_names
  from {table}
  where playername is null
  or playername in ('NA', 'N/A', 'na', 'n/a');
  """
  df = pd.read_sql(query, conn)
  return int(df.loc[0, "missing_names"])
miss = missing_names(conn)
print(f"There are {miss} athletes with missing or invalid names.")

#Q6. Function to find the number of athletes with data from multiple sources
def multiple_sources(conn):
  query = f"""
  select count(*) as num_players
  from (
  select playername
  from {table}
  group by playername
  having count(distinct data_source) >= 2) t;
  """
  df = pd.read_sql(query, conn)
  return int(df.loc[0, "num_players"])
player = multiple_sources(conn)
print(f"There are {player} athletes with data from multiple sources.")

# 1.3. Metric Discovery and Selection

# Query to find the top 10 most common metrics for Hawkins data 
sql_toexecute_metrics = """
SELECT
    data_source,
    metric AS metric_name,
    COUNT(*) AS record_count,
    MIN(timestamp) AS earliest_date,
    MAX(timestamp) AS latest_date,
    COUNT(DISTINCT timestamp) AS unique_dates
FROM research_experiment_refactor_test
WHERE data_source = 'hawkins'
GROUP BY metric
ORDER BY record_count DESC
LIMIT 10;
"""

metrics_response = pd.read_sql(sql_toexecute_metrics, conn)
metrics_response
print("Top 10 most common metrics for Hawkins data:")
print(metrics_response) 


# Query to find the top 10 most common metrics for Kinexon data
sql_toexecute_kinexon_metrics = """
SELECT
    data_source,
    metric AS metric_name,
    COUNT(*) AS record_count,
    MIN(timestamp) AS earliest_date,
    MAX(timestamp) AS latest_date,
    COUNT(DISTINCT timestamp) AS unique_dates
FROM research_experiment_refactor_test
WHERE data_source = 'kinexon'
GROUP BY metric
ORDER BY record_count DESC
LIMIT 10;
"""

kinexon_metrics_response = pd.read_sql(sql_toexecute_kinexon_metrics, conn)
kinexon_metrics_response 
print("Top 10 most common metrics for Kinexon data:")
print(kinexon_metrics_response)

# Query to find the top 10 most common metrics for Vald data
sql_toexecute_vald_metrics = """
SELECT
    data_source,
    metric AS metric_name,
    COUNT(*) AS record_count,
    MIN(timestamp) AS earliest_date,
    MAX(timestamp) AS latest_date,
    COUNT(DISTINCT timestamp) AS unique_dates
FROM research_experiment_refactor_test
WHERE data_source = 'Vald'
GROUP BY metric
ORDER BY record_count DESC
LIMIT 10;
"""

vald_metrics_response = pd.read_sql(sql_toexecute_vald_metrics, conn)
vald_metrics_response
print("Top 10 most common metrics for Vald data:")
print(vald_metrics_response)

# Query to find the number of unique metrics across all data sources
sql_to_execute_unique_metrics = """
SELECT
    COUNT(DISTINCT metric) AS total_unique_metrics,
    COUNT(DISTINCT CASE WHEN data_source = 'hawkins' THEN metric END) AS hawkins_unique_metrics,
    COUNT(DISTINCT CASE WHEN data_source = 'kinexon' THEN metric END) AS kinexon_unique_metrics,
    COUNT(DISTINCT CASE WHEN data_source = 'Vald' THEN metric END) AS vald_unique_metrics
FROM research_experiment_refactor_test;
"""

unique_metrics_response = pd.read_sql(sql_to_execute_unique_metrics, conn)
unique_metrics_response
print("Number of unique metrics across all data sources:")
print(unique_metrics_response) 

# 1.4. Metric Selection

# 1. 5 Metrics: Jump Height (m) and Peak Velocity (m/s) from Hawkins dataset, distance_total and accel_load_accum from Kinexon dataset, leftAvgForce/rightAverageForce from Vald dataset
#     a. Jump Height is commonly assessed in strength and conditioning fields, being used to measure the power of the lower-body and the vertical jump ability. Specifically, it measures the maximum vertical displacement of the center of mass (Nuzzo et al., 2011). 
#     It is important for athletic performance, because many professional sports organizations (e.g. NFL, NBA, etc.) use it to assess athlete potential, 