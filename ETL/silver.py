from pymongo import MongoClient
import pandas as pd
from pprint import pprint
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

col_names = ["icao24", "callsign", "time_position", "longitude", "latitude", "on_ground", "heading", "vertical_rate"]
cols = [0, 1, 3, 5, 6, 8, 10, 11]

client = MongoClient(os.environ["MONGO_URL"])
doc = client["airlines"]["states_all"].find_one({})
rows = [[state[i] for i in cols] for state in doc["states"]]
df = pd.DataFrame(rows, columns=col_names)

conn = psycopg2.connect(
    host="db.civmkvcgbklejootrkks.supabase.co",
    port=5432,
    database="postgres",
    user="postgres",
    password=os.environ["SUPABASE_DB_PASSWORD"]
)
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS states_all (
        icao24 TEXT, callsign TEXT, time_position BIGINT,
        longitude FLOAT, latitude FLOAT, on_ground BOOLEAN,
        heading FLOAT, vertical_rate FLOAT
    )
""")
execute_values(cur,
    "INSERT INTO states_all (icao24, callsign, time_position, longitude, latitude, on_ground, heading, vertical_rate) VALUES %s",
    df.values.tolist()
)
conn.commit()
cur.close()
conn.close()



print(f"Written {len(df)} rows to PostgreSQL.")

""" for state in doc["states"]:
    values = [state[i] for i in cols]
    pprint(values) """
#for doc in docs:
    #print(json.dumps(doc, default=str, indent=2))
#docs = client["airlines"]["states_all"].find({}, {"icao24": 1, "field2": 1, "_id": 0})
#df = pd.DataFrame(list(docs))

