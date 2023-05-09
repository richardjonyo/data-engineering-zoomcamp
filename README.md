# Data Engineering Zoomcamp Final Project

## Overview
This data pipeline project uses the [U.S. Energy Information Administration (EIA)'s](https://www.eia.gov/) weekly original estimates of state level coal production to generate coal production analytics acquired from the Mine Safety and Health Administration (MSHA). The pipeline focuses on using the weekly coal production datasets, which are publicly available in XLS format on the EIA website. The datasets are part of the weekly and monthly Coal Production dataset, which contains information on world energy statistics and is comprised of 22 CSV files.

This data pipeline transforms raw data into data ready for analytics with the intention of keeping data flowing to provide insights that lead to informed decisions. The goal of this project is to apply everything learned in the Data Engineering Zoomcamp course and build an end-to-end data pipeline.

## Datasets
The dataset has 54 columns - of which 53 are values for each week of year represented as "week 01", "week 02" etc . The following columns are used for the weekly data:

| #  | Attribute             |                     Description                                      |
|:--:|:---------------------:|----------------------------------------------------------------------|
|  1 | **state**                | A US state that produces coal.                  |
|  2 | **week 01 - week 53**          | Amount of coal produced in the week of a particular year (in thousand short tons)|
|  3 | **Total**          | Amount of coal produced in the particular year (in thousand short tons)|  


The following columns are used for the monthly data:

| #  | Attribute             |                     Description                                      |
|:--:|:---------------------:|----------------------------------------------------------------------|
|  1 | **state**                | A US state that produces coal.                  |
|  2 | **Jan - Dec**          | Amount of coal produced in the month of a particular year (in thousand short tons)| 
|  3 | **Total**          | The total amount of coal produced in the particular year (in thousand short tons)| 

Dataset Url: https://www.eia.gov/coal/production/weekly/includes/archive.php


## Problem statement
The goals of this pipeline is to:
* Extract coal production data from the Mine Safety and Health Administration website
* Transform the data to generate derived quantities for further analysis.</li>
* Load the transformed data in Big Query.</li>
* Orchestrate the pipeline using an orchestration tool.</li>
* Generate an online dashboard summarizing some of the findings.</li>

This project has the goal of answering the following questions among others:

1. What is the total estimated U.S. coal production in million short tons (MMst)
1. What is the average monthly or yearly tonange of coal produced in US since 2002?
2. Which is the highest coal producing state and region?

## Technologies used
* Google Cloud Platform (GCP): Cloud-based auto-scaling platform by Google
* Google Cloud Storage (GCS): Data Lake
* BigQuery: Data Warehouse
* Terraform: Infrastructure-as-Code (IaC)
* SQL: Data Analysis & Exploration
* Prefect (Cloud): Pipeline Orchestration
* DBT (Cloud): Data Transformation
* Spark: Distributed Processing 
* Pandas: Data Analysis & Exploration

## Data Pipeline Architecture
* The pipeline created for this project was for batch processing which runs periodically on a daily basis (for current year data) and once for historical data. The image below represents the architecture employed in this pipeline.
![image](./images/architecture.JPG)

### How the data pipeline works

* Prefect dataflows:

    1. [ETL Web to GCS](./prefect/etl_web_to_gcs.py): fetches xls files from the EIA website, extracts and transforms the data, and finally loads them into GCS bucket as csv files. It creates a three new fields to the dataframe (*'year'*, *'annual_average'*, and *'annual_total'*). The ETL process will execute once to obtain coal production data from the past (2001 up to 2022). Thereafter, it will retrieve weekly data solely for the present year.

    2. [ETL GCS to BigQuery](./prefect/etl_gcs_to_bq.py): fetches data from GCS, transforms the data by adding a 'year' column, and loads the data into BigQuery on the tables *'eia_week'* and *'eia_month'*. The ETL process will execute once to obtain coal production data from the past (2001 up to 2022). Thereafter, it will retrieve monthly data solely for the present year.

* Dbt Models and Seeds:

    1. [stg_eiadata](./dbt/stg_eiadata.sql): selects a all columns from the  staging table (stg_eiadata) that was loaded into BigQuery, and adds a unique key field. This file is located under the *'models/staging'* folder.

    2. [production_states](./dbt/production_states.sql): selects all state data from stg_eiadata, partitions it by year . Here, the partitioning makes it more efficient to query data and extract statistics by year. With respect to clustering, borough and state is the main categorical value but for this project we did not cluster the table a sit added no permormance benefit. This model is located under the *'models/core'* folder. The model employs a macro named [*'categorize_state'*](./dbt/categorize_state.sql) to distinguish the states from the regions.
	
	3. [production_regions](./dbt/production_regions.sql): selects all regional data from *'stg_eiadata'* and partitions the table by year . The tables is not clustered. This model is located in the *'models/core'* folder in the dbt folder.  The model employs a macro named [*'categorize_state'*](./dbt/categorize_state.sql) as above.

	4. [states_lookup](./dbt/states_lookup.csv) and [regions_lookup](./dbt/regions_lookup.csv): provide dictionary data for the states and regions. They are located in the 'models/seeds' folder in the dbt folder.

## Partitioning and Clustering:

- Partitioning was by column *'year'* to make it easier to manage and query the data. By dividing the table into smaller partitions, we improve query performance and control costs by reducing the number of bytes read by our query. 
- Clustering was not employed on this table since we have very few states, the tables are less than 1 GB and hence no query performance advantage.
- **Note:** It is important to note that tables with less than 1 GB don't show significant improvement with partitioning and clustering; doing so in a small table could even lead to increased cost due to the additional metadata reads and maintenance needed for these features (or) the processing data clustered and with out clustered is same for small data.

![image](./images/partitioned-table.JPG)

The final main table (eia_week) is of the format below after transformation:
<div align="left">
  
| #  | Attribute             |                     Description                                      |
|:--:|:---------------------:|----------------------------------------------------------------------|
|  1 | **eia_id**                | A unique id that identies the record.                  |
|  2 | **state**                | A US state or region that produces coal.                  |
|  3 | **week_1 - week_53**          | Amount of coal produced in the week (in thousand short tons) | 
|  4 | **year**          | The year of the coal production | 
|  5 | **annual_average**          | Annual average tons of coal production (in thousand short tons) | 
|  6 | **annual_total**          | Total average tons of coal production (in thousand short tons) | 
|  7 | **state_category**          | Distinguishes a state from a region) | 
 
The other two final tables (production_states and production_regions) are of the format below:
  
| #  | Attribute             |                     Description                                      |
|:--:|:---------------------:|----------------------------------------------------------------------|
|  1 | **state_code**  | A unique identifierl or a state or region.                  |
|  2 | **state**                | A US state or region that produces coal.                  |
|  3 | **year**          | The year of the coal production | 
|  4 | **region**          | Coal regions categories (West, South and Midwest) | 
|  5 | **division**          | Coal regions divisions: Pacific, Mountain, South Atlantic, East North Central, East South Central, West North Central, and West South Central | 
|  6 | **basin**          | Coal basins - Appalachian or Interior | 
|  7 | **east_west_mississippi_river**          | Denotes whether the state in located on the east or west of the Missipi River | 
|  8 | **annual_average**          | Annual average tons of coal production (in thousand short tons) | 
|  9 | **annual_total**          | Total average tons of coal production (in thousand short tons) | 
|  10 | **state_category**          | Category of state / region | 

</div>

## Steps for Project Reproduction
- Clone this repo.

- For reproducibility, have Docker (optional), Python (at least 3.9), Git and Terraform installed.

- Other tools and accounts required include a Google Cloud account, Prefect Cloud free account, and DBT Cloud developer account.


### Step 1:  Login to your Google Cloud Platform (GCP) account
- Go to your [GCP](https://cloud.google.com/) account.

### Step 2: Setup of a GCP Project
- Create a new GCP project. Take note of the Project ID. 
- Go to `IAM & Admin > Service accounts > Create service account`, provide a service account name and grant the roles `Viewer`, `BigQuery Admin`, `Storage Admin`, `Storage Object Admin`. 
- Download service account key locally, rename it to `google_credentials.json`. 
- Store it in your home folder for easier access. 
- Set and export the GOOGLE_APPLICATION_CREDENTIALS using `export GOOGLE_APPLICATION_CREDENTIALS=<path/to/your/service-account-authkeys>.json`
- Activate the service account using `gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS`
- Activate the following API's:
   * https://console.cloud.google.com/apis/library/iam.googleapis.com
   * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

### Step 3: Creation of GCP Infrastructure using Terraform
- [Install Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli)
- Change default variables `project`, `region`, `BQ_DATASET` in `variables.tf` (the file contains descriptions explaining these variables)
- Run the following commands from terraform directory on bash:
```shell
# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan

# Create new infra
terraform apply
```
- Confirm in GCP console that the infrastructure was correctly created.

### Step 4: Creation of Conda environment and Orchestration using Prefect flows.

#### Execution

- Create a Prefect Cloud accout and login. More information about Prefect Cloud is [here](https://docs.prefect.io/latest/cloud/cloud-quickstart/).

- Run the command below to login into Prefect cloud.
```
 prefect cloud login -k [api_key] 
 ```
- Register Prefect blocks. From Prefect dashboard navigate to blocks menu --> add `GCS Bucket` and provide below inputs.
	* Block name : `<your-GCS-bucket-block-name>`
	* Bucket name: `<your-bucket-name-created-by-terraform>`
	* GCP credentials:  Click on Add --> It opens up create block of GCP credentials , provide input below.
		* Block name : `<your-GCP-credentials-block-name>`
		* Service Account info: copy paste the json file data in the service account info.
		* Click on create.
	* GCP credentials:  Click on Add --> Select the above created `<your-GCP-credentials-block-name>`
	* Code generated needs to be replaced in the `web-to-gcs-parent.py` and `gcs_to_bq_parent`python files.
		```
		from prefect_gcp.cloud_storage import GcsBucket
		gcp_cloud_storage_bucket_block = GcsBucket.load("<your-gcp-bucket-block-name")

		from prefect_gcp import GcpCredentials
		gcp_credentials_block = GcpCredentials.load("<your-gcs-cred-block-name>")

		```    
- Run Prefect deployment:

   - Run the commands:
   ```
	python prefect/docker_deploy_to_gcs.py
	python prefect/docker_deploy_spark.py
    python prefect/docker_deploy_to_bq.py
   ```
   - Alternatively, from Prefect dashboard, go to Deployment and a start a quick run
   - The flows *'docker-eia-pcs-flow', docker-eia-spark-flow, and docker-eia-bq-flow* will be created.  Edit and schedule them to run once every week. For the parameter 'year' enter an array of years [2001, 2002, 2003 till 2023), for the parameter 'period' enter either 'week' or 'month'. Currently only week is applicable.

### Step 5: Batch processing and transformations using Spark

* Read more on [how to install Spark](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
* We use Spark to create a schema that is used to generate parquet files from csv files located on GCS and we save the final parquet files on the GCS. The schema ensures that all data that will go into our data warehouse will conform to the schema specified. This batch processing is orchestrated by a Prefect flow mentioned earlier.

### Step 6: Transformations using dbt

* Navigate to [dbt cloud](https://www.getdbt.com/) and create a new project by referring to this repository. Under the project subfolder update `/dbt`
* Select the BigQuery connection and update `service-account.json` file for the authentication. 
* Under dbt development menu, edit the `dbt-project.yml` to update the `name` and `models`.
* Add the following:		
	* [models/staging/stg_eiadata.sql](https://github.com/richardjonyo/data-engineering-zoomcamp/blob/main/dbt/stg_eiadata.sql)
	* [models/core/production_states.sql](https://github.com/richardjonyo/data-engineering-zoomcamp/blob/main/dbt/production_states.sql)
	* [models/core/production_regions.sql](https://github.com/richardjonyo/data-engineering-zoomcamp/blob/main/dbt/production_regions.sql)
	* [macros/categorize_state.sql](https://github.com/richardjonyo/data-engineering-zoomcamp/blob/main/dbt/categorize_state.sql) 
	* [seeds/states_lookup.csv](https://github.com/richardjonyo/data-engineering-zoomcamp/blob/main/dbt/states_lookup.csv) 
	* [seeds/regions_lookup.csv](https://github.com/richardjonyo/data-engineering-zoomcamp/blob/main/dbt/regions_lookup.csv) 
	* [packages.yml](https://github.com/richardjonyo/data-engineering-zoomcamp/blob/main/dbt/packages.yml)
	* [models/core/schema.yml](https://github.com/richardjonyo/data-engineering-zoomcamp/blob/main/dbt/schema.yml)
* Run below commands to execute the transformations:
	```
	dbt deps
	dbt build
	dbt run
	``` 
* The above will create dbt models and final tables
   **Note**: The transformations made were the selection of certain columns and creation of new ones.

## Dashboard

The final product for this pipeline was a dashboard built on Google Data Studio. The dashboard has three parts with control filters on year and state that demonstrate the analytics points below. [Link to the dashboard:](https://lookerstudio.google.com/reporting/5a912732-c32f-4857-9931-0af9213e8ffb)

## Future considerations

- Currently the project considers only weekly data, the monthly data could improve the analytics by displaying data summarized by month (Jan, Feb, Mar etc.). 
- Dockerize the set up

### Important note
Once you're done evaluating this project, stop and remove any cloud resources. Use Terraform to destroy your buckets and datasets with the command below:
```
terraform -chdir="./terraform" destroy -var="project=<project id here>"
```

