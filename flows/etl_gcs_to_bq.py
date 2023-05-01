from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints = True)
def extract_from_gcs(year: int, period: str, file: str) -> Path:
    """Download fhv data from GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=file, local_path=f"./data/eia/{period}/")
    return Path(f"./data/eia/{period}/{file}")

@task(log_prints=True)
def transform(df: pd.DataFrame, year:str) -> pd.DataFrame:
    """Add a new row for year"""
    df["year"] = year    
    return df


@task(log_prints = True)
def write_bq(df: pd.DataFrame, period: str) -> None:
    """Writing data into BigQuery"""
    gcp_credentials_block = GcpCredentials.load("gcs-service")
    df.to_gbq(destination_table="staging.eia_"+ period, 
              project_id = "dtc-gc", 
              credentials = gcp_credentials_block.get_credentials_from_service_account(), 
              chunksize=100000, 
              if_exists="append") #append data to the table if it exists

@flow(log_prints=True)
def etl_gcs_to_bq(years: list[int], period:str):
    """Main ETL Flow to load data to Big Query data warehouse"""  
    for year in years:  
        file = f"{period}prodforecast{year}tot.csv" 
        path_gcs = extract_from_gcs(year, period, file)
        df =  df = pd.read_csv(path_gcs)
        print(f"Row count: {len(df)}")
        transform(df, year)
        write_bq(df, period)

if __name__ == '__main__':
    period = "week" #'week' or 'month'
    years = [2022] 
    #years = [year for year in range(2023, 2001, -1)]   
    etl_gcs_to_bq(years, period)