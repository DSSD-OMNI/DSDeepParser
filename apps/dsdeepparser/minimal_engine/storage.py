import sqlite3
import os
import json

DB_PATH = os.environ.get("DB_PATH", "apps/dsdeepparser/minimal_engine/fpl_data.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT,
            payload TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_payload(endpoint: str, payload: dict):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO raw_data (endpoint, payload) VALUES (?, ?)",
        (endpoint, json.dumps(payload))
    )
    conn.commit()
    conn.close()
