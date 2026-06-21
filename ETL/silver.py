import logging
from pymongo import MongoClient
import pandas as pd
from pprint import pprint
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

col_names = ["icao24", "callsign", "time_position", "longitude", "latitude", "on_ground", "true_track", "vertical_rate"]
cols = [0, 1, 3, 5, 6, 8, 10, 11]

try:
    client = MongoClient(os.environ["MONGO_URL"])
    doc = client["airlines"]["states_all"].find_one({}, sort=[("fetched_at", -1)])
    rows = [[state[i] for i in cols] for state in doc["states"]]
    log.info(f"Fetched {len(rows)} rows from MongoDB.")
except KeyError as e:
    log.error(f"Missing environment variable: {e}")
    raise
except Exception as e:
    log.error(f"Failed to fetch data from MongoDB: {e}")
    raise

try:
    conn = psycopg2.connect(
        host=os.environ["SUPABASE_DB_URL"],
        port=5432,
        database="postgres",
        user="postgres",
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE map1")
    execute_values(cur,
        "INSERT INTO map1 (icao24, callsign, time_position, longitude, latitude, on_ground, true_track, vertical_rate, created_at) VALUES %s",
        [[*row, datetime.now(timezone.utc)] for row in rows]
    )
    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Written {len(rows)} rows to PostgreSQL.")
except KeyError as e:
    log.error(f"Missing environment variable: {e}")
    raise
except Exception as e:
    log.error(f"Failed to write to PostgreSQL: {e}")
    raise

