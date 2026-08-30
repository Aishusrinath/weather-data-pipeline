import json
from datetime import datetime
import os
import boto3


# BUCKET_NAME = "aishwarya-weather-data-lake-2026-01"
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
AWS_PROFILE = os.getenv("AWS_PROFILE")

def upload_weather_to_s3(data: dict) -> str:
    try:
        session = boto3.Session(profile_name="data-engineering")

        s3 = session.client(
            "s3",
            # region_name="ca-central-1"
            region_name=AWS_REGION

        )

        now = datetime.now()

        key = (
            f"raw/"
            f"year={now.strftime('%Y')}/"
            f"month={now.strftime('%m')}/"
            f"day={now.strftime('%d')}/"
            f"weather_{now.strftime('%Y%m%d_%H%M%S')}.json"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        return key
    except Exception as e:
        print(f"S3 UPLOAD ERROR: {e}")
        raise