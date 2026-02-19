import os, requests, json, time
API = os.getenv("API_URL", "http://localhost:8000")
def post(path, payload):
    r = requests.post(API + path, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

print(post("/ingest/csv", {"files": ["auth_events.csv"], "dataset": "auth"}))
print(post("/ingest/csv", {"files": ["payments.csv"], "dataset": "payments"}))
print(post("/ingest/csv", {"files": ["ip_geo.csv"], "dataset": "ip_geo"}))