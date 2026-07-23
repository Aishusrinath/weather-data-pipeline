from fastapi import FastAPI, HTTPException
from app.ingest import run_pipeline

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Weather Data Pipeline API"}

@app.post("/ingest")
def ingest(city: str):
    try:
        run_pipeline(city)
        return {"status": f"Data ingested for {city}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))