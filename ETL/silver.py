from pymongo import MongoClient
import pandas as pd
from pprint import pprint
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

load_dotenv()

col_names = ["icao24", "callsign", "time_position", "longitude", "latitude", "on_ground", "true_track", "vertical_rate"]
cols = [0, 1, 3, 5, 6, 8, 10, 11]

client = MongoClient(os.environ["MONGO_URL"])
doc = client["airlines"]["states_all"].find_one({})
rows = [[state[i] for i in cols] for state in doc["states"]]

conn = psycopg2.connect(
    host="db.civmkvcgbklejootrkks.supabase.co",
    port=5432,
    database="postgres",
    user="postgres",
    password=os.environ["SUPABASE_DB_PASSWORD"]
)
cur = conn.cursor()
execute_values(cur,
    "INSERT INTO map1 (icao24, callsign, time_position, longitude, latitude, on_ground, true_track, vertical_rate, created_at) VALUES %s",
    [[*row, datetime.now(timezone.utc)] for row in rows]
)
conn.commit()
cur.close()
conn.close()



print(f"Written {len(rows)} rows to PostgreSQL.")

