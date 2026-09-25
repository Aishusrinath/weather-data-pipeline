\# AWS Weather Data Engineering Pipeline



An end-to-end AWS data engineering project that ingests weather data from the OpenWeather API, stores raw data in an Amazon S3 data lake, processes new objects using AWS Lambda, catalogs processed data with AWS Glue, and performs serverless SQL analytics using Amazon Athena.



The project also includes a FastAPI ingestion endpoint and PostgreSQL integration for relational persistence.



\## Architecture



```text

&#x20;                   OpenWeather API

&#x20;                          |

&#x20;                          v

&#x20;                   Python / FastAPI

&#x20;                     /          \\

&#x20;                    v            v

&#x20;              PostgreSQL      Amazon S3

&#x20;                               raw/

&#x20;                                 |

&#x20;                          S3 ObjectCreated

&#x20;                                 |

&#x20;                                 v

&#x20;                            AWS Lambda

&#x20;                        Validation + Transform

&#x20;                          /             \\

&#x20;                         v               v

&#x20;                   processed/        rejected/

&#x20;                         |

&#x20;                         v

&#x20;                   AWS Glue Crawler

&#x20;                         |

&#x20;                         v

&#x20;                  Glue Data Catalog

&#x20;                         |

&#x20;                         v

&#x20;                   Amazon Athena

&#x20;                         |

&#x20;                         v

&#x20;                  SQL Analytics

&#x20;                         |

&#x20;                         v

&#x20;                athena-results/

```



\## Project Features



\- Extracts current weather data from the OpenWeather API

\- Provides a FastAPI endpoint for ingestion

\- Transforms API responses using Python and Pandas

\- Stores relational weather records in PostgreSQL

\- Stores original API responses in an Amazon S3 raw data layer

\- Partitions S3 objects by `year/month/day`

\- Uses an S3 `ObjectCreated` event to trigger AWS Lambda

\- Validates incoming weather JSON before processing

\- Routes valid records to the `processed/` data layer

\- Routes invalid records to the `rejected/` data layer

\- Records UTC processing and rejection timestamps

\- Logs accepted/rejected processing counts to CloudWatch Logs

\- Uses AWS Glue Crawler to discover schema and partitions

\- Registers processed data in the AWS Glue Data Catalog

\- Queries the S3 data lake using Amazon Athena

\- Includes partition-filtered and analytical SQL queries

\- Uses S3 lifecycle configuration for the raw data layer



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



\## S3 Data Lake Design



The S3 bucket is logically separated into the following prefixes:



```text

raw/

processed/

rejected/

athena-results/

```



Weather API responses are stored using date-based partitioning:



```text

raw/

└── year=YYYY/

&#x20;   └── month=MM/

&#x20;       └── day=DD/

&#x20;           └── weather\_YYYYMMDD\_HHMMSS.json

```



The Lambda function preserves the partition structure when writing valid records to `processed/` and invalid records to `rejected/`.



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



\## Amazon Athena Analytics



Amazon Athena queries the processed S3 data using the Glue Data Catalog table.



Example partition-aware query:



```sql

SELECT

&#x20;   city,

&#x20;   temperature,

&#x20;   humidity,

&#x20;   weather,

&#x20;   year,

&#x20;   month,

&#x20;   day

FROM weather\_data\_catalog.processed

WHERE year = '2026'

&#x20; AND month = '09';

```



Example aggregation:



```sql

SELECT

&#x20;   city,

&#x20;   ROUND(AVG(temperature), 2) AS avg\_temperature,

&#x20;   ROUND(AVG(humidity), 2) AS avg\_humidity,

&#x20;   COUNT(\*) AS observations

FROM weather\_data\_catalog.processed

GROUP BY city

ORDER BY city;

```



The project also demonstrates conversion of ISO-8601 strings for time-based analysis:



```sql

SELECT

&#x20;   city,

&#x20;   processed\_at,

&#x20;   from\_iso8601\_timestamp(processed\_at) AS processed\_timestamp

FROM weather\_data\_catalog.processed;

```



Additional queries are available in:



```text

sql/athena\_queries.sql

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



\- IAM user for development operations

\- Dedicated Lambda execution role

\- Dedicated Glue crawler service role

\- Scoped S3 permissions for service roles

\- `iam:PassRole` granted only where required

\- Environment secrets stored outside source control

\- `.env` excluded from Git

\- `.env.example` used as a configuration template



AWS access keys and application secrets must never be committed to this repository.



\## Cost Considerations



\### S3 partitioning



The data lake uses `year/month/day` partitions. Partition-aware Athena queries can avoid processing unrelated partitions as the dataset grows, reducing unnecessary data scanning.



\### Athena



Athena query efficiency depends heavily on the amount of data scanned. Queries should select only required columns and use partition predicates whenever appropriate.



The current project uses JSON because it keeps the learning pipeline simple. A future production optimization would convert processed records to a columnar format such as Parquet to reduce scan volume for analytical workloads.



\### S3 lifecycle



A lifecycle rule is configured for the `raw/` prefix to demonstrate storage lifecycle management.



Because individual weather JSON objects in this learning project are extremely small, transitioning them to another storage class may not be economically beneficial. A production design would evaluate object size, transition charges, retrieval patterns, and potentially compact small files before selecting lifecycle transitions.



\### Lambda



The Lambda function currently runs successfully with a small memory allocation. Memory, execution duration, and workload characteristics should be measured together before selecting production settings.



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



```text

datapipeline-project/

├── app/

│   ├── db.py

│   ├── ingest.py

│   ├── main.py

│   └── s3.py

│

├── lambda/

│   └── weather\_processor/

│       └── lambda\_function.py

│

├── sql/

│   ├── queries.sql

│   └── athena\_queries.sql

│

├── tests/

│   └── fixtures/

│       └── invalid-weather.json

│

├── .env.example

├── .gitignore

├── README.md

└── requirements.txt

```



\## Future Improvements



\- Convert the processed analytical layer from JSON to Parquet

\- Add automated unit and integration tests

\- Add infrastructure as code using AWS CloudFormation or Terraform

\- Add CI/CD for automated testing and deployment

\- Add data-quality metrics and alerting

\- Add dashboards for weather analytics

\- Evaluate streaming ingestion when Kinesis access is available



\## Key Learning Outcomes



This project demonstrates hands-on experience with:



\- REST API ingestion

\- Python ETL development

\- Relational persistence

\- S3 data lake organization

\- Event-driven processing

\- Data validation and rejected-record handling

\- AWS IAM and service roles

\- AWS Glue metadata discovery

\- Serverless SQL analytics with Athena

\- Partition-aware querying

\- CloudWatch-based debugging

\- AWS cost-awareness
