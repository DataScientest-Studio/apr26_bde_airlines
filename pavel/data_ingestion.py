import os
import json
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient, errors as mongo_errors

# =============================================================================
# ENVIRONMENT
# Load .env file from same directory as script
# =============================================================================
load_dotenv()

# =============================================================================
# CONFIGURATION
# All constants in one place
# =============================================================================
BASE_URL      = "https://opensky-network.org/api"
BEGIN         = 1778536800
END           = 1778623200
AIRPORT       = "EDDF"
ICAO24        = "3c675a"

# Timestamped run folder — each run gets its own clean directory
BASE_DIR      = "opensky_data"
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR    = os.path.join(os.path.dirname(__file__), f"run_{RUN_TIMESTAMP}")

# Bounding box around Germany/Frankfurt (~50 sq° → 2 credits vs 4 for global)
BBOX = {
    "lamin": 47.0,   # south edge
    "lamax": 52.0,   # north edge
    "lomin": 5.0,    # west edge
    "lomax": 15.0,   # east edge
}

# =============================================================================
# LOGGING SETUP
# Main log  → pipeline.log (root level)
# Credit log → opensky_data/run_TIMESTAMP/credits.log
# Both loggers write to console as well
# =============================================================================

# Main logger — initialised immediately at module level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Credit logger — initialised after output dir is created in setup_output_dir()
credit_log = None

def setup_credit_logger():
    """
    Set up dedicated logger for credit-related info.
    Writes to credits.log inside the current run folder.
    Avoids duplicate handlers if called more than once.
    """
    logger = logging.getLogger("credits")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        credit_log_path = os.path.join(OUTPUT_DIR, "credits.log")
        handler = logging.FileHandler(credit_log_path)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)

    return logger


# =============================================================================
# TOKEN MANAGER
# Handles OAuth2 token lifecycle — auto-refreshes before expiry
# =============================================================================
TOKEN_URL            = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
TOKEN_REFRESH_MARGIN = 30  # seconds before expiry to proactively refresh

class TokenManager:
    def __init__(self):
        self.token      = None
        self.expires_at = None

    def get_token(self):
        """Return valid token, refreshing if expired or missing."""
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        """Fetch a new access token from OpenSky auth server."""
        try:
            r = requests.post(TOKEN_URL, data={
                "grant_type":    "client_credentials",
                "client_id":     os.environ["OPENSKY_CLIENT_ID"],
                "client_secret": os.environ["OPENSKY_CLIENT_SECRET"],
            })
            r.raise_for_status()
            data            = r.json()
            self.token      = data["access_token"]
            expires_in      = data.get("expires_in", 1800)
            self.expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
            log.info("Token refreshed successfully.")
            return self.token
        except requests.exceptions.HTTPError as e:
            log.error(f"Token refresh failed — HTTP error: {e}")
            raise
        except KeyError as e:
            log.error(f"Missing environment variable: {e}. Ensure OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET are set in .env")
            raise

    def headers(self):
        """Return Authorization header with current valid token."""
        return {"Authorization": f"Bearer {self.get_token()}"}


# =============================================================================
# MONGODB SETUP
# =============================================================================
def get_db():
    """Connect to MongoDB and return the airlines database."""
    try:
        client = MongoClient(os.environ["MONGO_URL"], serverSelectionTimeoutMS=5000)
        # Ping to confirm connection is alive before proceeding
        client.admin.command("ping")
        log.info("MongoDB connection established.")
        return client["airlines"]
    except KeyError:
        log.error("MONGO_URL not set. Ensure it is defined in .env")
        raise
    except mongo_errors.ServerSelectionTimeoutError as e:
        log.error(f"Could not connect to MongoDB: {e}")
        raise


# =============================================================================
# FILE SETUP
# Create timestamped run folder and initialise credit logger
# =============================================================================
def setup_output_dir():
    """
    Create timestamped run folder inside opensky_data/.
    Initialise credit logger once folder exists.
    """
    global credit_log
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log.info(f"Output directory ready: {OUTPUT_DIR}/")
    log.info(f"Absolute path: {os.path.abspath(OUTPUT_DIR)}")
    credit_log = setup_credit_logger()


# =============================================================================
# FILE WRITER
# Each file named after its endpoint — folder timestamp separates runs
# =============================================================================
def save_to_file(name, data):
    """
    Write data to a JSON file inside the current run folder.
    Filename is the endpoint name — run folder provides the timestamp.
    """
    path = os.path.join(OUTPUT_DIR, f"{name}.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        log.info(f"File saved: {path}")
        return path
    except OSError as e:
        # Non-fatal — log and continue, DB write may still succeed
        log.warning(f"Could not write file for '{name}': {e}")
        return None


# =============================================================================
# DATABASE WRITER
# Inserts with full audit fields for data lineage
# =============================================================================
def save_to_db(db, collection_name, data, pipeline_run_id):
    """
    Insert data into MongoDB with audit fields:
      - fetched_at      : when the API was called
      - load_date       : when the record was written to DB
      - source          : which endpoint the data came from
      - pipeline_run_id : unique ID tying all records from this run together

    Uses ordered=False on bulk insert to skip duplicates and continue.
    """
    if not data:
        log.warning(f"No data to insert into '{collection_name}' — skipping.")
        return

    collection = db[collection_name]
    fetched_at = datetime.utcnow().isoformat()
    load_date  = datetime.utcnow().isoformat()

    try:
        if isinstance(data, list):
            if len(data) == 0:
                log.warning(f"Empty list for '{collection_name}' — skipping.")
                return

            # Stamp every document with audit fields
            docs = [{
                **doc,
                "fetched_at":      fetched_at,
                "load_date":       load_date,
                "source":          collection_name,
                "pipeline_run_id": pipeline_run_id,
            } for doc in data]

            result = collection.insert_many(docs, ordered=False)
            log.info(f"Inserted {len(result.inserted_ids)} documents into '{collection_name}'.")

        elif isinstance(data, dict):
            data.update({
                "fetched_at":      fetched_at,
                "load_date":       load_date,
                "source":          collection_name,
                "pipeline_run_id": pipeline_run_id,
            })
            result = collection.insert_one(data)
            log.info(f"Inserted 1 document into '{collection_name}' (id: {result.inserted_id}).")

        else:
            log.warning(f"Unexpected data type for '{collection_name}': {type(data)} — skipping.")

    except mongo_errors.BulkWriteError as e:
        # Some inserts may fail due to duplicates — log count but don't crash
        write_errors = e.details.get("writeErrors", [])
        log.warning(f"{len(write_errors)} duplicate(s) skipped in '{collection_name}'.")
    except mongo_errors.PyMongoError as e:
        log.error(f"MongoDB insert failed for '{collection_name}': {e}")


# =============================================================================
# CREDIT MONITOR
# Writes to credits.log in run folder AND main pipeline.log
# =============================================================================
def log_credits(response, name):
    """
    Read and log rate limit headers from OpenSky response.
    Credit info goes to dedicated credits.log in run folder.
    Summary also written to main pipeline.log.
    """
    remaining   = response.headers.get("X-Rate-Limit-Remaining")
    retry_after = response.headers.get("X-Rate-Limit-Retry-After-Seconds")

    if remaining:
        credit_log.info(f"[{name}] Credits remaining: {remaining}")
        log.info(f"[{name}] Credits remaining: {remaining}")

    if retry_after:
        credit_log.warning(f"[{name}] Rate limited — retry after: {retry_after}s")
        log.warning(f"[{name}] Rate limited — retry after: {retry_after}s")


# =============================================================================
# CORE FETCH FUNCTION
# Fetches from API then saves to file AND MongoDB
# =============================================================================
def fetch_and_store(name, url, db, pipeline_run_id, params=None):
    """
    Fetch data from OpenSky API endpoint then:
      1. Validate the response
      2. Save to local JSON file inside timestamped run folder
      3. Insert into MongoDB collection with audit fields

    Args:
        name            : collection name and output filename
        url             : full API endpoint URL
        db              : MongoDB database object
        pipeline_run_id : unique ID for this pipeline run
        params          : optional query parameters dict
    """
    log.info(f"Fetching '{name}' — params={params}")

    try:
        r = requests.get(url, params=params, headers=tokens.headers(), timeout=10)

        # Log credits after every call
        log_credits(r, name)

        # Handle known cases explicitly before raise_for_status
        if r.status_code == 404:
            log.warning(f"404 Not Found for '{name}' — no data for given params.")
            return

        if r.status_code == 429:
            retry = r.headers.get("X-Rate-Limit-Retry-After-Seconds", "unknown")
            log.warning(f"429 Rate Limited for '{name}' — retry after {retry}s.")
            return

        # Raises HTTPError for all other 4xx/5xx
        r.raise_for_status()

        # Validate response is parseable JSON
        try:
            data = r.json()
        except ValueError:
            log.error(f"Response for '{name}' is not valid JSON — skipping.")
            return

        # Validate response is not empty
        if data is None or data == [] or data == {}:
            log.warning(f"Empty response for '{name}' — nothing to store.")
            return

        # Save to file and DB
        save_to_file(name, data)
        save_to_db(db, name, data, pipeline_run_id)

    except requests.exceptions.ConnectionError:
        log.error(f"Connection error for '{name}' — check internet or API status.")
    except requests.exceptions.Timeout:
        log.error(f"Request timed out for '{name}' — API may be slow.")
    except requests.exceptions.HTTPError as e:
        log.error(f"HTTP error for '{name}': {e}")


# =============================================================================
# MAIN PIPELINE
# =============================================================================
def main():
    # Unique ID for this run — ties all records and files together
    pipeline_run_id = f"run_{RUN_TIMESTAMP}"

    log.info("=" * 60)
    log.info(f"PIPELINE START — {pipeline_run_id}")
    log.info("=" * 60)

    # Setup folders and loggers
    setup_output_dir()

    # Connect to MongoDB
    db = get_db()

    endpoints = [
        # Bounding box keeps cost at 2 credits instead of 4 for global
        ("states_all",       f"{BASE_URL}/states/all",        BBOX),

        # Flight-level data — costs 4 credits each (live/<24h window)
        ("flights_all",      f"{BASE_URL}/flights/all",       {"begin": BEGIN, "end": END}),
        ("flights_aircraft", f"{BASE_URL}/flights/aircraft",  {"icao24": ICAO24, "begin": BEGIN, "end": END}),
        ("arrivals",         f"{BASE_URL}/flights/arrival",   {"airport": AIRPORT, "begin": BEGIN, "end": END}),
        ("departures",       f"{BASE_URL}/flights/departure", {"airport": AIRPORT, "begin": BEGIN, "end": END}),

        # Experimental — may 404 if aircraft not currently flying
        ("tracks",           f"{BASE_URL}/tracks/all",        {"icao24": ICAO24, "time": 0}),
    ]

    for name, url, params in endpoints:
        fetch_and_store(name, url, db, pipeline_run_id, params)

    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE — {pipeline_run_id}")
    log.info("=" * 60)


if __name__ == "__main__":
    tokens = TokenManager()
    main()