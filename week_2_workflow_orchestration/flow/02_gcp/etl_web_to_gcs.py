from pathlib import Path
import pandas as pd 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
import os 
from prefect.tasks import task_input_hash
from datetime import timedelta
from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
   """
   Read data from web into pandas dataframe
   """

   #if randint(0, 1) > 0:
   #   raise Exception
   
   print(dataset_url)

   df = pd.read_csv(dataset_url)
   print("fetched df")
   return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
   """
   Fix some dtype issues 
   """
   df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
   df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

   print(df.head(2))
   print(f"Columns: {df.dtypes}")
   print(f"Rows: {len(df)}")
   return df 

@task 
def write_local(df: pd.DataFrame, color:str, dataset_file:str) -> Path:
   """
   Write dataframe as a parquet file 
   """
   # os.mkdir("data")
   # os.mkdir("data/yellow")
   path = Path(f"data/{color}/{dataset_file}.parquet")
   df.to_parquet(path, compression="gzip")

   return path

@task
def write_gcs(path: Path) -> None:
   """
   Upload local parquet files to google cloud 
   """
   gcs_block = GcsBucket.load("nytaxi-gcs")
   print(path)
   gcs_block.upload_from_path(
      from_path=f"{path}",
      to_path=path
   )

   return
   

# to parametrize we can make this as a subflow
@flow
def etl_web_to_gcs(year: int, month: int, color: str) -> None:

   dataset_file = f"{color}_tripdata_{year}-{month:02}"

   dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

   df = fetch(dataset_url)
   df_clean = clean(df)
   path = write_local(df_clean, color, dataset_file)
   write_gcs(path)

@flow()
def etl_parent_flow(color, months, year):
   months = [2,3]
   color = "yellow"
   year = 2021

   for month in months:
      print(month)
      etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
   etl_parent_flow(color, months, year)
