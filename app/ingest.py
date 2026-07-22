import requests
import pandas as pd
import os
from dotenv import load_dotenv
from app.db import engine

load_dotenv()

API_KEY = os.getenv("API_KEY")

def fetch_weather():
    city = "Toronto"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"

    response = requests.get(url)
    data = response.json()

    return data


def transform_data(data):
    df = pd.DataFrame([{
        "city": data["name"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["description"]
    }])

    return df


def load_data(df):
    df.to_sql("weather_data", engine, if_exists="append", index=False)


def run_pipeline():
    data = fetch_weather()
    df = transform_data(data)
    load_data(df)