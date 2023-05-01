# Data Engineering Zoomcamp Final Project (Feb - May 2023)

## Overview
This data pipeline project intends to use the U.S. Energy Information Administration (EIA)'s weekly and monthly original estimates of state level coal production to generate more accurate estimates using quarterly mine level coal production data from the Mine Safety and Health Administration (MSHA). The estimates are guaranteed to conform with the MSHA survey data and include refuse coal. The project will focus on using the weekly and monthly coal production datasets, which are publicly available in XLS format on the EIA website. The datasets are part of the Weekly Coal Production dataset, which contains information on world energy statistics and is comprised of 11 CSV files.

This data pipeline transforms raw data into data ready for analytics, applications, machine learning and AI systems. The intention is to keep data flowing to solve problems, inform decisions, and, make our lives more convenient. The goal of this project is to apply everything I have learned in the Data Engineering Zoomcamp course and build an end-to-end data pipeline.

## Datasets
The dataset has 54 columns - of which 53 are values for each week of year. The following columns will be used:

<div align="center">
  
| #  | Attribute             |                     Description                                      |
|:--:|:---------------------:|----------------------------------------------------------------------|
|  1 | **state**                | US state that produces coal.                  |
|  2 | **week 1 - week 12**          | Week of the year and | 
  
</div>

Url: https://www.eia.gov/coal/production/weekly/includes/archive.php


## Problem statement
The project involved the following: 

* Selecting a dataset that one is interested in
* Creating a pipeline for processing the dataset and placing it to a datalake (Google Cloud Storage)
* Creating a pipeline for moving the data from the lake to a data warehouse (Google BigQuery)
* Transforming the data in the data warehouse (DBT and prepare it for the dashboard 
* Creating a dashboard (Google Data Studio)

## Technologies used
* Google Cloud Platform (GCP): Cloud-based auto-scaling platform by Google
* Google Cloud Storage (GCS): Data Lake
* BigQuery: Data Warehouse
* Terraform: Infrastructure-as-Code (IaC)
* Docker: Containerization
* SQL: Data Analysis & Exploration
* Prefect (Cloud): Pipeline Orchestration
* DBT (Cloud): Data Transformation
* Spark: Distributed Processing 
* Pandas: Data Analysis & Exploration

## Data Pipeline Architecture
* The pipeline created for this project was for batch processing which runs periodically on a daily basis. The image below represents the architecture used in this project.
<p align="center">
  <img width="100%" src="images/architecture.jpg"/>
</p>

## Partitioning and Clustering:
![image](https://user-images.githubusercontent.com/69354054/231012117-8d3dc96a-9e35-4443-84a6-e05cd30cbf29.png)

- Partition by column **start_date**, more specifically by **year** to obtain annual granularity
- Clustering by column **country** to group data that have the same country value

## Dashboard

The final product for this pipeline was a dashboard built on Google Data Studio. The dashboard contains a graph that shows the distribution of some categorical data and the distribution of the data across a temporal line.

## Peer review
* The project involved peer review of three projects by evauationg agains a set evaluation criteria.
