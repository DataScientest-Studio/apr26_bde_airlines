import os
import requests
from datetime import datetime, timedelta
from pymongo import MongoClient

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID = os.environ["OPENSKY_CLIENT_ID"]
CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]

TOKEN_REFRESH_MARGIN = 30


class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self):
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
        r.raise_for_status()
        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
        return self.token

    def headers(self):
        return {"Authorization": f"Bearer {self.get_token()}"}


tokens = TokenManager()

response = requests.get(
    "https://opensky-network.org/api/flights/arrival?airport=EDDF&begin=1778536800&end=1778623200",
    headers=tokens.headers(),
)
response.raise_for_status()
flights = response.json()

client = MongoClient(os.environ["MONGO_URL"])
db = client["airlines"]
collection = db["arrivals"]

if flights:
    result = collection.insert_many(flights)
    print(f"Inserted {len(result.inserted_ids)} flights into MongoDB.")
else:
    print("No flights returned from API.")
