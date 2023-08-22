from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta
from datetime import datetime
import os


@task(log_prints = True)
def extract_from_gcs(year: int, period: str, file: str) -> Path:
    """Downloads parquet files from GCS"""

    gcs_path = f"data/spark/{period}/"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=gcs_path)
    return Path(f"./{gcs_path}{file}")


@task(log_prints=True)
def transform(df: pd.DataFrame, year:str) -> pd.DataFrame:
    """Transforms the dataframe by adding three (3) new columns"""
     # add a new column for year
    df["year"] = year 
    print(df.head())
    print(df.tail())
    week_columns = [col for col in df.columns if col.startswith('week_')]
    df_weeks = df[week_columns]

    # add column for average and total
    annual_average = df_weeks.mean(axis=1)
    annual_total = df_weeks.sum(axis=1)
    df['annual_average'] = annual_average
    df['annual_total'] = annual_total

    # add column for modified datetime
    df['modified'] = datetime.now()
   
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
def etl_parent_flow(years: list[int], period:str):
    """Main ETL Flow to load data to Big Query data warehouse"""  
    count = 0
    try:
        for year in years:  
            file = f"{period}prodforecast{year}tot.parquet" 
            path_gcs = extract_from_gcs(year, period, file)
            df =  pd.read_parquet(path_gcs)
            print(f'PARQUET PATH:{path_gcs}')

            if df.empty:            
                print("Empty dataframe...")
            else:
                print(f"ROW COUNT: {len(df)}")
                transform(df, year)
                write_bq(df, period)
    except Exception as e:
        print(f"HTTP error occurred for gcs-to-bq on {datetime.now().date()}: {e}")
        return None
    

if __name__ == '__main__':
    period = "week" #'week' or 'month'
    #first run 2002 to create the full scheme, the current one might not be complete
    #years = [2002] 
    years = [year for year in range(2023, 2002, -1)]   
    etl_parent_flow(years, period)