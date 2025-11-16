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
  limit 1;
  """
  df = pd.read_sql(query, conn)
  return df
records = most_records(conn)
source = records['data_source'].iloc[0]
print(f"The data source with the most records is {source}")

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
