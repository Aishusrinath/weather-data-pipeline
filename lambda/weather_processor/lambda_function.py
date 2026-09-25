import json
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3


s3 = boto3.client("s3")

RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/")
PROCESSED_PREFIX = os.getenv("PROCESSED_PREFIX", "processed/")
REJECTED_PREFIX = os.getenv("REJECTED_PREFIX", "rejected/")


def lambda_handler(event, context):
    accepted = 0
    rejected = 0

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        print(f"Processing object: s3://{bucket}/{key}")

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            raw_body = response["Body"].read().decode("utf-8-sig")
            data = json.loads(raw_body)

            # Validate expected OpenWeather structure
            if (
                data.get("name")
                and "main" in data
                and "temp" in data["main"]
                and "humidity" in data["main"]
                and data.get("weather")
                and "description" in data["weather"][0]
            ):
                processed_data = {
                    "city": data["name"],
                    "temperature": data["main"]["temp"],
                    "humidity": data["main"]["humidity"],
                    "weather": data["weather"][0]["description"],
                    "source_key": key,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }

                output_key = key.replace(
                    RAW_PREFIX,
                    PROCESSED_PREFIX,
                    1,
                )

                s3.put_object(
                    Bucket=bucket,
                    Key=output_key,
                    Body=json.dumps(processed_data),
                    ContentType="application/json",
                )

                accepted += 1
                print(f"ACCEPTED: {output_key}")

            else:
                raise ValueError("Required weather fields are missing")

        except Exception as e:
            rejected += 1

            rejected_data = {
                "source_key": key,
                "reason": str(e),
                "rejected_at": datetime.now(timezone.utc).isoformat(),
            }

            output_key = key.replace(
                RAW_PREFIX,
                REJECTED_PREFIX,
                1,
            )

            s3.put_object(
                Bucket=bucket,
                Key=output_key,
                Body=json.dumps(rejected_data),
                ContentType="application/json",
            )

            print(f"REJECTED: {output_key} - {e}")

    print(
        f"Processing summary: accepted={accepted}, "
        f"rejected={rejected}"
    )

    return {
        "statusCode": 200,
        "accepted": accepted,
        "rejected": rejected,
    }