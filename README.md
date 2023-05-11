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
* Docker: Containerization
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

## Partitioning and Clustering

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

## Steps to Reproduce the Project

- To reproduce the pipeline [follow the instructions here](./setup.md).


## Dashboard

The final product for this pipeline was a dashboard built on Google Data Studio. The dashboard has three parts with control filters on year and state that demonstrate the analytics points below. [Link to the dashboard:](https://lookerstudio.google.com/reporting/5a912732-c32f-4857-9931-0af9213e8ffb)

Here is a screenshot for the dashboard generated.

![image](./images/dashboard.jpg)

## Future considerations

- Currently the project considers only weekly data, the monthly data could improve the analytics by displaying data summarized by month (Jan, Feb, Mar etc.). 

### Important note
Once you're done evaluating this project, stop and remove any cloud resources. Use Terraform to destroy your buckets and datasets with the command below:
```
terraform -chdir="./terraform" destroy -var="project=<project id here>"
```

