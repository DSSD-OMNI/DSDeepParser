from fastapi import FastAPI
import threading
from .engine import loop

app = FastAPI(title="DSDeepParser Minimal Engine")

@app.get("/")
def root():
    return {"service": "DSDeepParser", "mode": "minimal"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/metrics")
def metrics():
    return {"status": "ok"}

def start_engine():
    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

start_engine()
