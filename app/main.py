from fastapi import FastAPI
from app.ingest import run_pipeline

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Weather Data Pipeline API"}

@app.post("/ingest")
def ingest(city: str):
    run_pipeline(city)
    return {"status": f"Data ingested for {city}"}