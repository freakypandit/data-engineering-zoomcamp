from pathlib import Path
import pandas as pd 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
import os 

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
   """
   Read data from web into pandas dataframe
   """

   df = pd.read_csv(dataset_url)
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
   os.mkdir("data")
   os.mkdir("data/yellow")
   path = Path(f"data/{color}/{dataset_file}.parquet")
   df.to_parquet(path, compression="gzip")

   return path

@task
def write_gcs(path: Path) -> None:
   """
   Upload files to google cloud 
   """

   

@flow
def etl_web_to_gcs() -> None:
   color = "yellow"
   year = 2021
   month=1
   dataset_file = f"{color}_tripdata_{year}-{month:02}"

   dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

   df = fetch(dataset_url)
   df_clean = clean(df)
   write_local(df_clean, color, dataset_file)

if __name__ == "__main__":
   etl_web_to_gcs()
