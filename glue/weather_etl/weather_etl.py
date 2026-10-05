import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp


# Get Glue job parameters
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_TARGET_PATH"]
)

# Initialize Spark and Glue
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# Read processed weather data from the Glue Data Catalog
weather_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database="weather_data_catalog",
        table_name="processed"
    )
    .toDF()
)


# Select and clean the columns needed for analytics
curated_df = (
    weather_df
    .select(
        col("city"),
        col("temperature").cast("double"),
        col("humidity").cast("int"),
        col("weather"),
        col("processed_at"),
        col("year"),
        col("month"),
        col("day")
    )
    .filter(col("city").isNotNull())
    .filter(col("temperature").isNotNull())
    .filter(col("humidity").isNotNull())
    .withColumn(
        "processed_at",
        to_timestamp(col("processed_at"))
    )
)


# Write curated data to S3 in Parquet format
(
    curated_df.write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(args["S3_TARGET_PATH"])
)


job.commit()