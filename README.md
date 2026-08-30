# Weather Data Pipeline

## Overview
This project builds an end-to-end data pipeline using a weather API.

## Features
- Extracts data from OpenWeather API
- Transforms using Python (Pandas)
- Loads into PostgreSQL
- Queries using advanced SQL
- Exposes ingestion via FastAPI

## Tech Stack
Python, SQL, FastAPI, PostgreSQL

## Run Instructions
1. Add API key in .env
2. Run: uvicorn app.main:app --reload
3. Hit POST /ingest