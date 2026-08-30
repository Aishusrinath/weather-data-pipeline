import requests
import pandas as pd
import os
from dotenv import load_dotenv
from app.db import engine


from app.s3 import upload_weather_to_s3

load_dotenv(override=True)

API_KEY = os.getenv("API_KEY")

# print("LOADED API KEY:", API_KEY)

def fetch_weather(city: str):

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"

    response = requests.get(url)

    response.raise_for_status()

    data = response.json()

    s3_key = upload_weather_to_s3(data)

    print(f"Uploaded to S3: {s3_key}")

    print("API RESPONSE:", data)

    return data


def transform_data(data):
    if data.get("cod") != 200:
        raise ValueError(f"Weather API failed: {data}")

    df = pd.DataFrame([{
        "city": data.get("name"),
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["description"]
    }])

    return df

def load_data(df):
    try:
        df.to_sql("weather_data", engine, if_exists="append", index=False)
    except Exception as e:
        print("DB ERROR:", e)
        raise

def run_pipeline(city: str):
    data = fetch_weather(city)
    df = transform_data(data)
    load_data(df)

    return data