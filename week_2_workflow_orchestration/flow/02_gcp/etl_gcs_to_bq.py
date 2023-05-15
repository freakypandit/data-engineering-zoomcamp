from pathlib import Path
import pandas as pd 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
   """Download trip data from GCS"""
   gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
   gcs_block = GcsBucket.load("nytaxi-gcs")

   print(gcs_path)
   gcs_block.get_directory(from_path=gcs_path, local_path=f"downloads")
   return Path(f"{gcs_path}")

@task()
def transform(path:Path) -> pd.DataFrame:
   """Data cleaning on our file"""
   df = pd.read_parquet(path)
   print(f"pre: missing passenger count:{df['passenger_count'].isna().sum()}")
   df['passenger_count'].fillna(0, inplace=True)

   print(f"pre: missing passenger count:{df['passenger_count'].isna().sum()}")
   return df

@task()
def write_bq(df: pd.DataFrame) -> Path:
   """Write our clean data to BigQuery"""

   df.to_gbq(
      destination_table="dezoomcamp.rides",
      project_id="dezoomcamptaxi",
      credentials=GcpCredentials.load("zoom-gcp-creds").get_credentials_from_service_account(),
      chunksize=500_000,
      if_exists="append"
   )

   return "dezoomcamptaxi.dezoomcamp.rides"

# using the pandas to_gbq for loading data to bigquery
@flow()
def etl_gcs_to_bq():
    """Main ETL Flow to load data into bigquery"""
    color="yellow"
    year=2021
    month=1

    path = extract_from_gcs(color, year, month)
    data = transform(path)
    path = write_bq(data)

    print("Loaded to:", path)

if __name__ == "__main__":
   etl_gcs_to_bq()
