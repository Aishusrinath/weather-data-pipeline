\# AWS Weather Data Engineering Pipeline

An end-to-end AWS data engineering project that ingests weather data from the OpenWeather API, stores raw events in an Amazon S3 data lake, validates and transforms new objects using AWS Lambda, catalogs datasets with AWS Glue, performs PySpark ETL using AWS Glue, stores an analytics-ready curated layer in Snappy-compressed Parquet, and queries the data using Amazon Athena.

The project implements a layered data-lake architecture with `raw/`, `processed/`, `rejected/`, and `curated/` zones. The curated layer is partitioned by `year/month/day` and optimized for analytical workloads.

The project also includes a FastAPI ingestion endpoint and PostgreSQL integration for relational persistence.



\## Architecture



```text

&#x20;                                      OpenWeather API
                           |
                           v
                    Python / FastAPI
                      /          \
                     v            v
               PostgreSQL      Amazon S3
                                raw/
                                  |
                           S3 ObjectCreated
                                  |
                                  v
                             AWS Lambda
                         Validate + Transform
                           /             \
                          v               v
                    processed/        rejected/
                          |
                          v
                 AWS Glue Crawler
                          |
                          v
                 Glue Data Catalog
                  processed table
                          |
                          v
                  AWS Glue PySpark ETL
                          |
                          v
                    Amazon S3
                     curated/
               Snappy Parquet files
                          |
                 year/month/day
                    partitions
                          |
                          v
                 AWS Glue Crawler
                          |
                          v
                 Glue Data Catalog
                   curated table
                          |
                          v
                    Amazon Athena
                          |
                          v
                    SQL Analytics
\## Project Features



## Project Features

- Extracts current weather data from the OpenWeather API
- Provides a FastAPI endpoint for ingestion
- Transforms API responses using Python and Pandas
- Stores relational weather records in PostgreSQL
- Stores original API responses in an Amazon S3 `raw/` data layer
- Partitions S3 objects by `year/month/day`
- Uses an S3 `ObjectCreated` event to trigger AWS Lambda
- Validates incoming weather JSON before processing
- Routes valid records to the `processed/` data layer
- Routes invalid records to the `rejected/` data layer
- Records UTC processing and rejection timestamps
- Logs accepted/rejected processing counts to CloudWatch Logs
- Uses AWS Glue Crawler to discover processed JSON schema and partitions
- Registers processed data in the AWS Glue Data Catalog
- Uses an AWS Glue 5.1 PySpark ETL job to transform processed JSON into an analytics-ready curated dataset
- Casts analytical fields to appropriate data types and converts `processed_at` to a timestamp
- Writes curated data as Snappy-compressed Parquet
- Partitions the curated layer by `year/month/day`
- Uses a second Glue Crawler to catalog the curated Parquet dataset
- Queries both processed and curated data using Amazon Athena
- Includes partition-filtered and aggregation SQL queries
- Uses S3 lifecycle configuration for the raw data layer
- Uses scoped IAM service roles for Lambda, Glue Crawlers, and Glue ETL

\## Technology Stack



\*\*Languages and frameworks\*\*



\- Python

\- SQL

\- FastAPI

\- Pandas

\- SQLAlchemy



\*\*Database\*\*



\- PostgreSQL



\*\*AWS\*\*



\- Amazon S3

\- AWS Lambda

\- AWS Glue

\- Amazon Athena

\- Amazon CloudWatch

\- AWS IAM


## S3 Data Lake Design

The S3 bucket is logically separated into the following prefixes:

```text
raw/
processed/
rejected/
curated/
athena-results/
```

Each prefix represents a different stage of the data pipeline:

- `raw/` — original OpenWeather API responses
- `processed/` — validated and transformed JSON records produced by Lambda
- `rejected/` — invalid records with rejection metadata
- `curated/` — analytics-ready Snappy-compressed Parquet produced by AWS Glue ETL
- `athena-results/` — Athena query result files

Raw weather API responses are stored using date-based partitioning:

```text
raw/
└── year=YYYY/
    └── month=MM/
        └── day=DD/
            └── weather_YYYYMMDD_HHMMSS.json
```

The Lambda function preserves this partition structure when writing records to `processed/` or `rejected/`.

The Glue ETL job reads the cataloged `processed` dataset and writes the curated analytical layer as Parquet:

```text
curated/
└── year=YYYY/
    └── month=MM/
        └── day=DD/
            └── part-*.snappy.parquet
```

The partition values in the curated layer are inherited from the source `processed` dataset. They represent the source ingestion partition and are therefore distinct from `processed_at`, which records when Lambda processed the record.



\## Lambda Processing



New JSON files uploaded under `raw/` automatically trigger the `weather-data-processor` Lambda function.



The function:



1\. Reads the new S3 object.

2\. Decodes and parses the JSON.

3\. Validates required OpenWeather fields.

4\. Transforms valid records into an analytics-friendly structure.

5\. Writes valid data to `processed/`.

6\. Writes invalid data and its rejection reason to `rejected/`.

7\. Logs accepted and rejected counts to CloudWatch.



The S3 prefixes are configured using Lambda environment variables:



```text

RAW\_PREFIX=raw/

PROCESSED\_PREFIX=processed/

REJECTED\_PREFIX=rejected/

```



This avoids embedding environment-specific prefix configuration directly into the processing logic.



\## AWS Glue Data Catalog



An AWS Glue Crawler scans:



```text

s3://<bucket-name>/processed/

```



The crawler registers the dataset in:



```text

Database: weather\_data\_catalog

Table: processed

```



Glue automatically discovered the JSON schema and the following partition keys:



```text

year

month

day

```



The actual weather records remain in S3; the Glue Data Catalog stores metadata used by analytical services such as Athena.

## AWS Glue PySpark ETL

The integration phase adds an AWS Glue 5.1 Spark ETL job named `weather-curated-etl`.

The job reads the `processed` table from the AWS Glue Data Catalog and performs the following transformations:

1. Selects the analytical weather fields.
2. Casts `temperature` to `double`.
3. Casts `humidity` to `int`.
4. Filters records with null values in required analytical fields.
5. Converts `processed_at` from a string to a Spark timestamp.
6. Writes the result as partitioned Parquet to the S3 `curated/` layer.

The output is partitioned by:

```text
year
month
day
```

Spark writes the Parquet output using Snappy compression.

A second Glue Crawler, `weather-curated-crawler`, scans the curated S3 layer and registers:

```text
Database: weather_data_catalog
Table: curated
```

The resulting curated schema contains:

```text
city            string
temperature     double
humidity        int
weather         string
processed_at    timestamp
year            string    partition key
month           string    partition key
day             string    partition key
```

The deployed ETL job was successfully executed and the resulting Parquet objects, catalog table, schema, and partitions were verified before querying the curated dataset with Athena.

## Amazon Athena Analytics

Amazon Athena queries both the processed JSON dataset and the curated Parquet dataset through the AWS Glue Data Catalog.

The curated table is the preferred analytical layer because it stores typed, columnar Parquet data partitioned by `year/month/day`.

### Query curated weather records

```sql
SELECT
    city,
    temperature,
    humidity,
    weather,
    processed_at,
    year,
    month,
    day
FROM weather_data_catalog.curated
ORDER BY processed_at DESC
LIMIT 10;
```

### Partition-aware query

Partition predicates allow Athena to restrict the query to relevant S3 partitions:

```sql
SELECT
    city,
    temperature,
    humidity,
    weather,
    processed_at
FROM weather_data_catalog.curated
WHERE year = '2026'
  AND month = '09'
  AND day = '02';
```

This query was tested successfully and returned the record stored in the selected partition.

### Analytical aggregation

```sql
SELECT
    city,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    COUNT(*) AS record_count
FROM weather_data_catalog.curated
GROUP BY city
ORDER BY city;
```

The aggregation was tested successfully against the curated Parquet dataset.

Additional Athena queries are stored in:

```text
sql/athena_queries.sql
sql/curated_queries.sql
```

\## Data Quality and Failure Handling



The Lambda function validates that incoming records contain the expected weather fields before they enter the processed layer.



Valid records follow:



```text

raw/ -> processed/

```



Invalid records follow:



```text

raw/ -> rejected/

```



Rejected records include the original S3 source key, rejection reason, and UTC rejection timestamp.



A deliberately invalid JSON fixture is included under:



```text

tests/fixtures/

```



to document and test the rejection path.



\## Security and IAM



The project follows role-based AWS access rather than embedding AWS credentials in application code.



Key practices include:



- IAM user for development operations
- Dedicated Lambda execution role
- Dedicated Glue crawler service role
- Dedicated Glue ETL service role
- Least-privilege S3 permissions scoped to required prefixes
- Lambda reads from `raw/` and writes only to `processed/` and `rejected/`
- Glue Crawler reads only the datasets it needs to catalog
- Glue ETL reads from `processed/` and writes to `curated/`
- `iam:PassRole` granted only for required service roles
- Environment secrets stored outside source control
- `.env` excluded from Git
- `.env.example` used as a configuration template

AWS access keys and application secrets must never be committed to this repository.



\## Cost Considerations



\### S3 partitioning



The data lake uses `year/month/day` partitions. Partition-aware Athena queries can avoid processing unrelated partitions as the dataset grows, reducing unnecessary data scanning.



\### Athena



Athena query efficiency depends heavily on the amount of data scanned. Queries should select only required columns and use partition predicates whenever appropriate.



The current project uses JSON because it keeps the learning pipeline simple. A future production optimization would convert processed records to a columnar format such as Parquet to reduce scan volume for analytical workloads.

### Athena and Parquet

Athena query efficiency depends on the amount of data scanned. Queries should select only required columns and use partition predicates whenever appropriate.

The pipeline retains JSON in the `processed/` layer for validation and intermediate processing, while the `curated/` analytical layer uses Snappy-compressed Parquet.

Parquet provides a columnar representation that allows analytical engines such as Athena to read only required columns rather than scanning complete JSON records. Combined with `year/month/day` partitioning, this design is intended to reduce unnecessary data scanning as the dataset grows.

Because the current learning dataset is very small, scan-size comparisons between JSON and Parquet are not representative of production-scale cost savings.

\### S3 lifecycle



A lifecycle rule is configured for the `raw/` prefix to demonstrate storage lifecycle management.



Because individual weather JSON objects in this learning project are extremely small, transitioning them to another storage class may not be economically beneficial. A production design would evaluate object size, transition charges, retrieval patterns, and potentially compact small files before selecting lifecycle transitions.



\### Lambda



The Lambda function currently runs successfully with a small memory allocation. Memory, execution duration, and workload characteristics should be measured together before selecting production settings.

### AWS Glue ETL

AWS Glue Spark ETL uses provisioned processing capacity while a job is running, so ETL runs are performed intentionally rather than unnecessarily repeating jobs during development.

The verified integration run used two G.1X workers, completed successfully in approximately one minute, and consumed approximately 0.036111 DPU-hours.

For a production workload, job duration, worker type, worker count, partition strategy, and incremental processing should be monitored and tuned together.

The current learning implementation uses an overwrite write mode for the curated layer. A production implementation could use incremental processing, job bookmarks, append/upsert strategies, or another appropriate data-management pattern to avoid rewriting the full curated dataset.


\## What Broke and How I Fixed It



\### AWS authentication



AWS CLI sessions occasionally expired during development.



\*\*Resolution:\*\* Reauthenticated using the dedicated development profile and verified the active identity using AWS STS before continuing.



\### IAM permissions



Several AWS operations initially failed because the development IAM user did not have permission to create or pass service roles.



\*\*Resolution:\*\* Created dedicated service roles administratively and granted narrowly scoped `iam:PassRole` permissions where required.



\### Lambda invalid-file decoding



An invalid test JSON file created from PowerShell contained a UTF-8 byte order mark (BOM), causing JSON decoding to fail before the intended schema validation.



\*\*Resolution:\*\* Changed the Lambda decoder to:



```python

.decode("utf-8-sig")

```



The invalid record then reached schema validation correctly and was routed to `rejected/`.



\### Glue crawler creation



The Glue crawler initially failed to be created because the IAM user lacked `iam:CreateRole`.



\*\*Resolution:\*\* Created a dedicated Glue service role separately, attached the required Glue and S3 read permissions, and configured the crawler to use the existing role.



\### Athena authorization



Athena Query Editor initially returned an authorization error for `athena:GetWorkGroup`.



\*\*Resolution:\*\* Added Athena permissions for the development environment and verified access to the `primary` workgroup.



\### Athena query result location



Athena required an S3 destination for query results before queries could execute.



\*\*Resolution:\*\* Configured a dedicated:



```text

athena-results/

```



prefix to keep analytical query outputs separate from source datasets.

### Curated crawler S3 permissions

The existing Glue crawler service role originally had permission to read only the `processed/` prefix. When the curated Parquet crawler was introduced, the role also needed access to the `curated/` layer.

**Resolution:** Extended the crawler role's scoped S3 permissions to list and read both `processed/` and `curated/` while avoiding broad bucket-wide S3 permissions.

### Streaming service availability

The planned streaming extension using Amazon Kinesis could not be deployed in the current AWS account configuration.

**Resolution:** Kept Kinesis as a future streaming extension and completed the integration project using the existing FastAPI/OpenWeather ingestion path, Lambda, S3, Glue Data Catalog, Glue PySpark ETL, Parquet, and Athena. This keeps the repository accurate about what was actually implemented and tested.

\## Local Setup



Create and activate a virtual environment:



```powershell

python -m venv venv

.\\venv\\Scripts\\Activate.ps1

```



Install dependencies:



```powershell

pip install -r requirements.txt

```



Create `.env` using `.env.example` as the template.



Do not commit `.env`.



Run the FastAPI application:



```powershell

uvicorn app.main:app --reload

```



The ingestion endpoint can then be used to request weather ingestion for a city.



\## Project Structure


datapipeline-project/
├── app/
│   ├── db.py
│   ├── ingest.py
│   ├── main.py
│   └── s3.py
│
├── lambda/
│   └── weather_processor/
│       └── lambda_function.py
│
├── glue/
│   └── weather_etl/
│       └── weather_etl.py
│
├── sql/
│   ├── queries.sql
│   ├── athena_queries.sql
│   └── curated_queries.sql
│
├── tests/
│   └── fixtures/
│       └── invalid-weather.json
│
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt



\## Future Improvements



- Add automated unit and integration tests
- Add infrastructure as code using AWS CloudFormation or Terraform
- Add CI/CD for automated testing and deployment
- Add data-quality metrics and alerting
- Add dashboards for weather analytics
- Replace full curated-layer overwrite processing with incremental ETL
- Evaluate AWS Glue job bookmarks for incremental processing
- Address the small-files problem through compaction as data volume grows
- Evaluate streaming ingestion with Amazon Kinesis when service access is available


\## Key Learning Outcomes



This project demonstrates hands-on experience with:



- REST API ingestion
- Python and Pandas data transformation
- FastAPI application development
- Relational persistence with PostgreSQL
- S3 data lake organization
- Layered `raw/`, `processed/`, `rejected/`, and `curated/` data design
- Event-driven processing with S3 and Lambda
- Data validation and rejected-record handling
- AWS IAM and least-privilege service roles
- AWS Glue Crawlers and Data Catalog
- PySpark ETL development with AWS Glue
- JSON-to-Parquet analytical transformation
- Snappy-compressed Parquet storage
- Partitioned data-lake design
- Serverless SQL analytics with Amazon Athena
- Partition-aware querying and aggregation
- CloudWatch-based debugging
- AWS cost-awareness and ETL capacity considerations