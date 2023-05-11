from pathlib import Path
import pandas as pd
import pyarrow as pa
import numpy as np
import datetime
import pyarrow.parquet as pq
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

    
@task(log_prints=True, retries=2)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read coal production estimates data from web into a pandas DataFrame"""
    print(f"File URL: {dataset_url}")
    try:
        df = pd.read_excel(dataset_url, engine='xlrd')
    except pd.errors.EmptyDataError:
        print("The file is empty.")
        return None  
    except pd.errors.ParserError as e:
        print(f"Error parsing the file: {str(e)}")
        return None  
    except pd.errors.DtypeWarning:
        print("Warning: a column has an unexpected data type.")
        return None  
    except FileNotFoundError as e:
        print(f"Error file not found: {str(e)}")
        return None   
    except Exception as e:
        print(f"Error fetching DataFrame: {e}")
        return None 
    return df

@task()
def write_local(df: pd.DataFrame, year: str, period: str) -> Path:
    """Write DataFrame out locally"""
    # Create a folder 'data/raw/week' and 'data/raw/month' in the working directory before running this code
    path = Path(f"data/raw/{period}/{period}prodforecast{year}tot.csv")   
    print(f"PATH: {path.as_posix()}")
    df = clean(df, year, period)
    df.to_csv(path, index=False)
    return path

def clean(df: pd.DataFrame, year: str, period: str) -> pd.DataFrame:
    current_year = datetime.date.today().year
    if period == 'week':
        #Files for 2013 to date uses same excel format - only rename columns
        rename_years = [year for year in range(current_year, 2013, -1)] 
        if year in rename_years:
            # Rename the column names from the format: "04/01/2023  (week 13)" to the format: "week_1"
            cols = list(df.columns)
            cols[1:] = [f'week_{week}' for week in range(1, len(df.columns))]
            df.columns = cols
        else:
            # Delete the first row for the rest of the files
            #df = df.dropna(subset=[df.columns[0]])
            df = df.drop(0)
            column_names = {}
            for i, col in enumerate(df.columns[1:], start=1):
                week_number = i
                new_col = f"week_{week_number}"
                column_names[col] = new_col
            column_names[df.columns[0]] = "state"            
            df = df.rename(columns=column_names)
    else: #month
        # Rename the month column names from the format: "Jan 2022" to the format: "Jan"
        column_names = {} 
        new_names = {col: col.split()[0] for col in df.columns}
        df = df.rename(columns=new_names)
        
        # For columns before 2014
        rename_month = [year for year in range(2013, 2002, -1)] 
        new_names = {col: col.split('-')[0] for col in df.columns}
        df = df.rename(columns=new_names)
        
        column_names[df.columns[0]] = "state"       
        if year != current_year:
            column_names[df.columns[-1]] = "total"
        df = df.rename(columns=column_names)
            
    # Replace all occurrences of (".") with  ("") an empty string
    df = df.replace(r'\.', '', regex=True)  
       
    # Remove empty columns
    df.dropna(how='all', inplace = True) 
    return df


@task()
def write_to_gcs(path: Path) -> None:
    """Upload local file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path) 
    return


@flow(log_prints=True)
def etl_web_to_gcs(year, period) -> pd.DataFrame:
    """The Main ETL function"""    
    dataset_url = f"https://www.eia.gov/coal/production/weekly/current_year/{period}prodforecast{year}tot.xls"
    print(f"URL: {dataset_url}")
    df = fetch(dataset_url)
    
    # If dataframe is empty use the archive url
    if df is None:
         print(f"Dataset prodforecast{year}tot.xls is empty")
         dataset_url = f"https://www.eia.gov/coal/production/weekly/archive/{period}prodforecast{year}tot.xls"
         df = fetch(dataset_url)
         
    return df  
    
@flow(log_prints=True)
def etl_parent_flow(years: list[int], period: str, writelocal: bool):
    for year in years:
        df = etl_web_to_gcs(year, period) #period represents either 'week' or 'month'
        path = write_local(df, year, period) 
        write_to_gcs(path.as_posix())
       

if __name__ == '__main__':
    period = "week" #'week' or 'month'
    years = [2022] 
    #years = [year for year in range(2013, 2001, -1)]
    writelocal = False    
    etl_parent_flow(years, period, writelocal)