
# MBITION TECHNICAL CHALLENGE SOLUTION

In this project, I developed an end-to-end data-warehouse pipeline to integrate and analyse the GitHub issues and pull requests.


### Architecture
In this project, I implement the ETL Pipeline Implementation using Medallion Architecture design pattern. This architecture logically organizes the data into progressive layers (Bronze->Silver->Gold) based on the quality of the data. In this architecture, we ensure Data Quality improvement in each layer. The data is easy to query instead of complex joins as compared to traditional dwh architecture. For data governance, we can restrict and provide data access easily using this layered approach.

The below architecture diagram presents the proposed architecture
 
![architecture.jpg](images%2Farchitecture.jpg)

In this project the Layers Bronze, Silver, and Gold data present as `raw`,  `sta` and `stg` respectively. 

**Bronze Layer (Raw Data)**: Landing layer with untouched as-it-is data. In our given problem statement the GitHub Pull Request Data Analysis `issues` and `pull_requests` data are provided in a `.json.zip` format. The data is stored in a directory and can be considered as a RAW Layer for us. In real-world project, we could use AWS S3 or any cloud storage to persist the raw data. 

**Silver Layer (Filtered and Clean Data)**: Define structured and clean data and evolve with the requirements. The given data (issues and pull_requests) is enriched with a schema with required columns only. The representation of this layer is prefixed with the `sta_` schema in a data warehouse. De-Duplicated is being performed here in this layer. The data is integrated as an Incremental Approach daily date-wise checkpoint in `sta_ ` and guarantees uniqueness on daily integrated data.

**Gold Layer (Business Level Aggregation)**: Deliver incrementally clean aggregated data to the downstream application. The enriched schema from the Silver `sta_` schema is used as a source to generate pull request metrics. The gold layer represents `stg_` as a prefix for the dwh schema.

#### Choice of Technologies:
Pyspark is used to implement the Data Integration in all these three layers. Postgres is used to build DWH as data management. Although we could use here AWS Redshift. Due to limited personal account access to AWS, I decided to use PostgreSQL which I can deploy locally via Docker and run and test the application. However, in current development switching to a different DWH will be straightforward and configurable via config_prod.json.

For the DWH Serving Layer Jupyter is used

### Logical Database Design
These dwh layers are created based on the following required dwh metrics.
How long do pull requests (PRs) stay open on average?
What is the average time interval between an issue being created and the corresponding PR being merged?
Which labels are most commonly assigned to PRs, and what is their relative distribution?
What is the average and median of the total number of lines changed per milestone?
Considering these statements, I have created the following data model which will simplify the analysis with this pre-aggregated data.

![dependency_graph.jpg](images%2Fdependency_graph.jpg)

### DWH Consideration:

In the data source, the `number` attribute uniquely identifies the issue and pull_request within its repository. Thus "number" and "repo_name" is the main Primary Key constraints throughout the Data model creation.


According to the GitHub docs https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#get-an-issue

```
Every pull request is an issue, but not every issue is a pull request. 
For this reason, "Issues" endpoints may return both issues and pull requests in the response. 
```
To answer metric number 1,2&4) Linked between Issue and PR 
the pull_requests linked with the issue is extracted from the issue.pull_requests 
But this key only provides a URL. To extract further pull_request data that needs to join with the `pull_requests` table. This is why `stg_github.pull_requests_metrics` is required. The granularity of the `pull_request_id` also calculated the time interval between the linked issue being created and to pull_request being merged. 

3) The labels are available as nested array JSON structure in pull_requests.json source. This needs to flatten and finally calculate for each label count the number of pull_requests this label is assigned. This aggregate data is available  in `stg_github.labels_metrics`

The tables/jobs dependency graph is shown in the following diagram. And detailed DDL script is provided in this create_table.sql script.


### Development Paradigm
The project structure uses Functional Programming because it is the best favour with PySpark native implementation of Immutability, Parallelism reducing the side effects occurring and finally stateless programming.

#### Project Structure:
The Project directory describes as follows

```
mbition_technical_challenge
      |__ aws-terraform          # AWS Terraform implementation for prod deployment 
      |__ data                   # Source data .zip
      |__ data-integration       # Python Project for Data Integration
      |__ dwh                    # Shared database directory for postgres volume and DDL Statement
      |__ images                 # images for the architecture
      |__ notebook               # Jupyter Notebook for Serving Layer with PySpark connection
      |__ docker-compose.yml     # Local Infrastructure provisioning for Redshift and Jupyter
      |__ docker-compose-spark.yml # Start Local Standalone Spark Cluster
      |__ Makefile               # Makefile
      |__ Readme.md              # Solution Description of challenge
```

#### Data Integration
This is main PySpark jobs implementation
```
data-integration
    |__data                 # Extracted main data source directory in .json format
    |__dist                 # PySpark Package files in .zip
    |__jobs                 # PySpark Jobs  
    |__lib                  # Shared lib folder
    |__resources            # JAR Required for Spark Extra Path
    |__test                 # PySPARK jobs Tests
    |__config.json          # Spark, Jobs and DWH configuration for local 
    |__config_prod.json     # Spark, Jobs and DWH configuration for prod or any other environment
    |__main.py              # Main Spark Driver Progrm
    |__Makefile             # Data Preparation and spark submit commands recipes

```

#### Project Execution in Action:


1. Pre-Requisites: `make`, `docker` and `docker-compose` is required to run this project in local.

2. First of all download the .zip data from here https://github.com/mbition-videowall-team/data  and copy it to `data/` folder. This .zip file need to extracted. This can be done via 
```
make prepare-data
```
3. Start Infrastructure PostgreSQL and Jupyter Notebook. The DWH Enforced Schema automatically created on container startup
```
make deploy-local-infra
```

Please wait until the PostgrsSQL container state change to `running` 

4. Then run spark job locally via `make run-local`


###  Deploying via Spark Standalone Cluster

1. First Build Python Package

```
make  package
```
2. Start the spark cluster with 1 Master and 1 Worker node using following make command
```
make  deploy-spark
```
3. Submit Spark Job

```
make deploy-job job_name=sta_github_issues
```

### AWS Deployment

Our Data Integration PySpark jobs can be schedule via AWS EMR Serverless Job. For this the following will be architecture.
S3 Bucket: To Provision Raw Data and PySpark Package
AWS Step Function: Schedule EMR Serverless Jobs
EMR Serverless: Run PySpark JOb
RDS:PostgreSQL/Redshift: As as DWH

I start creating the terraform module implementation to deploy these AWS Services via IaC. But this is incomplete and has 
partial implementation of 
1. S3 creation with data source upload.
2. EMR Serverless Application creation

Missing:
1. AWS Step Function to Schedule/deploy spark job to EMR Serverless
2. RDS/REDSHIFT Provisioning

### 2. DWH Analysis

The Queries which can answer the required question is available in Jupyter Notebook [github_pull_request_analysis.ipynb](notebook%2Fgithub_pull_request_analysis.ipynb)

### 3. Future Work or Known Lack of Features

1. Implementation of Unit Testing
2. Automate the table partitioning via some scheduled jobs
3. Logging and Monitoring
4. Implementation of end-to-end terraform Pyspark job scheduling via AWS Step Functions. 

NOTE: I properly test this project locally, if there is any issue raise during the project execution. Please let me know
i will be able to resolve it faster.
