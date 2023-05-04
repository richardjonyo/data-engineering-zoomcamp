# Data Engineering Zoomcamp Final Project

## Overview
This data pipeline project uses the [U.S. Energy Information Administration (EIA)'s](https://www.eia.gov/) weekly and monthly original estimates of state level coal production to generate more estimates using quarterly mine level coal production data from the Mine Safety and Health Administration (MSHA). The project focuses on using the weekly and monthly coal production datasets, which are publicly available in XLS format on the EIA website. The datasets are part of the weekly and monthly Coal Production dataset, which contains information on world energy statistics and is comprised of 22 CSV files.

This data pipeline transforms raw data into data ready for analytics with the intention of keeping data flowing to provide insights that lead to informed decisions. The goal of this project is to apply everything learned in the Data Engineering Zoomcamp course and build an end-to-end data pipeline.

## Datasets
The dataset has 54 columns - of which 53 are values for each week of year represented as "week 01", "week 02" etc . The following columns are used:

<div align="center">
  
| #  | Attribute             |                     Description                                      |
|:--:|:---------------------:|----------------------------------------------------------------------|
|  1 | **state**                | A US state that produces coal.                  |
|  2 | **week 1 - week 12**          | Week of the year | 
|  3 | **year**          | Captures the year when the coal production data estimate | 
  
</div>

Dataset Url: https://www.eia.gov/coal/production/weekly/includes/archive.php


## Problem statement
The goals of this pipeline is to:
* Extract coal production data from the Mine Safety and Health Administration
* Transform the data to generate derived quantities for further analysis.</li>
* Load the transformed data in Big Query.</li>
* Orchestrate the pipeline using an orchestration tool.</li>
* Generate a report summarizing some of the findings.</li>

This project has the goal of answering the following questions among others:

1. What is the estimated totaled U.S. coal production in million short tons (MMst)
1. What is the average yearly tonange of coal produced in US since 2002?
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

    1. [ETL Web to GCS](./prefect/etl_web_to_gcs.py): fetches xls files from the EIA website, extracts and transforms the data, and finally loads them into GCS bucket as csv files. It creates a three new fields to the dataframe (*'year'*, *'annual_average'*, and *'annual_total'*). This ETL will run once to extract the historical coal production data (2001 to 2023). Consequently it will only be pulling data for the current year.

    2. [ETL GCS to BigQuery](./prefect/etl_gcs_to_bq.py): fetches data from GCS, transforms the data by adding a 'year' column, and loads the data into BigQuery on the tables *'eia_week'* and *'eia_month'*. This ETL will run once to extract the historical coal production data (2001, 2021, 2022, till 2022). Consequently it will only be pulling data for the current year.

* Dbt models:

    1. [stg_eiadata](./tranformers/models/staging/stg_eiadata.sql): selects a all columns from the  staging table (stg_eiadata) that was loaded into BigQuery, and adds a unique key field.

    2. [production_states](./tranformers/models/core/production_states.sql): selects all state data from stg_eiadata, partitions it by year . Here, the partitioning makes it more efficient to query data and extract statistics by year. With respect to clustering, borough and state is the main categorical value but for this project we did not cluster the table a sit added no permormance benefit.
		* The model employs a macro named [*'get_state_category'*](./tranformers/macros/core/get_state_category.sql) to distinguish the states from the regions
	
	3. [production_regions](./tranformers/models/core/production_regions.sql): selects all regional data from stg_eiadata, partitions it by year . The tables is also partitioned by year and not clustered.
		* The model employs a macro named [*'get_state_category'*](./tranformers/macros/core/get_state_category.sql) as above.

## Partitioning and Clustering:

- Partitioning was by column *'year'* to make it easier to manage and query the data. By dividing the table into smaller partitions, we can improve query performance and control costs by reducing the number of bytes read by a query. 
- Clustering was not employed on this table since we have very few states and hence no query performance advantage. If we had numerous states then the table would be clustered by the column *'state'* to group data that have the same state value.
- **Note:** It is important to note that tables with less than 1 GB don't show significant improvement with partitioning and clustering; doing so in a small table could even lead to increased cost due to the additional metadata reads and maintenance needed for these features (or) the processing data clustered and with out clustered is same for small data.

![image](./images/partitioned-table.JPG)

## Steps for Project Reproduction
- Clone this repo.

- For reproducibility, have Docker, Python (at least 3.9), Git and Terraform installed.

- Other tools and accounts required include a Google Cloud account, Prefect Cloud free account, and DBT developer account.


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
   - The flows *'docker-eia-pcs-flow', docker-eia-spark-flow, and docker-eia-bq-flow* will be created.  Edit and schedule them to run once every week.For parameter 'year' enter an array of years (2001 - 2023), for the parameter 'period' enter either 'week' or 'month'. 

### Step 5: Batch processing and transformations using Spark

* We use Spark to create a schema that is used to generate parque files from csv files located on GCS and we save the parque files on the GCS. This ensures that all data that wil go into our datawarehouse wil conform to this schema. This batch processing is orchestrated by a Prefect flow.


### Step 6: Transformations using dbt

* Navigate to [dbt cloud](https://www.getdbt.com/) and create a new project by referring to this repository. Under the project subfolder update `/dbt`
* Select the BigQuery connection and update `service-account.json` file for the authentication. 
* Under dbt development menu, edit the `dbt-project.yml` to update the `name` and `models`.
* Add the following:
	* [macros/get_state_category.sql](https://github.com/richardjonyo/data-engineering-zoomcamp/dbt/macros) 
	* [models/core/schema.yml](https://github.com/richardjonyo/data-engineering-zoomcamp/dbt/models/core/schema.yml)
	* [models/staging/stg_eiadata.sql](https://github.com/richardjonyo/data-engineering-zoomcamp/dbt/models/staging/stg_eiadata.sql)
	* [models/core/production_states.sql](https://github.com/richardjonyo/data-engineering-zoomcamp/dbt/models/core/production_states.sql)
	* [models/core/production_regions.sql](https://github.com/richardjonyo/data-engineering-zoomcamp/dbt/models/core/production_regions.sql)
	* [packages.yml](https://github.com/richardjonyo/data-engineering-zoomcamp/dbt/packages.yml)
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

A preview of the dashboard can also be seen below:

## Future considerations

- Currently the project considers only weekly data, the monthly data could improve the analytics. 
- Dockerize the set up

### Important note
Once you're done evaluating this project, stop and remove any cloud resources. Use Terraform to destroy your buckets and datasets with the command below:
```
terraform -chdir="./terraform" destroy -var="project=<project id here>"
```

