
# Steps to Reproduce this Pipeline

For reproducibility, clone this repo, have Docker, Python (at least 3.9), Git and Terraform installed.

Other tools and accounts required include a Google Cloud account, Prefect Cloud free account, Docker Hub, and DBT Cloud developer account.


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

### Deploy image to Docker Hub
- Create a Dockerfile in your root folder by runnning the command below:
 ```
touch Dockerfile
 ```
- Build and push the image to Docker Hub using the commands below:
 ```
docker image build -t <dockerhub username>/prefect:<image-name> .
docker image push <dockerhub username>/prefect:<image-name>
 ```
- Our flows will be copied to the path *'/opt/prefect/flows/'* and our data will be copied to the path *"/opt/prefect/data/'* on Docker Hub

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