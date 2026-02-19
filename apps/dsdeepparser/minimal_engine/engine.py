import requests
import time
from .storage import init_db, save_payload

BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
FIXTURES_URL = "https://fantasy.premierleague.com/api/fixtures/"

def fetch_and_store(url, name):
    print(f"Fetching {name}...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    save_payload(name, data)
    print(f"Saved {name}")

def run_once():
    init_db()
    fetch_and_store(BOOTSTRAP_URL, "bootstrap")
    fetch_and_store(FIXTURES_URL, "fixtures")

def loop():
    while True:
        run_once()
        print("Sleeping 6 hours...")
        time.sleep(21600)
