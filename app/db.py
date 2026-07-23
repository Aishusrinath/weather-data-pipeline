import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(override=True)

DB_URL = os.getenv("DB_URL")

engine = create_engine(DB_URL)