# path: connectors/rest_stub/client.py
from __future__ import annotations
import os
import time
import random
from typing import Dict, Iterable, Any, List
import requests
from datetime import datetime, timedelta

API_BASE = os.getenv("LIZARD_API_BASE", "http://localhost:8000")

def fetch_stub_events(n: int = 200) -> List[Dict[str, Any]]:
    now = datetime.utcnow()
    out = []
    for i in range(n):
        out.append({
            "ts": (now - timedelta(minutes=n - i)).isoformat() + "Z",
            "source": "rest_stub",
            "event_type": random.choice(["auth_success","auth_failure"]),
            "user_id": f"user{random.randint(1,10)}",
            "account_id": f"acc{random.randint(1,5)}",
            "device_id": f"dev{random.randint(1,8)}",
            "card_hash": None,
            "ip": f"192.0.2.{random.randint(1,254)}",
            "geo_lat": 48.85 + random.uniform(-0.1,0.1),
            "geo_lon": 2.35 + random.uniform(-0.1,0.1),
            "country": "FR",
            "city": "Paris",
            "is_unusual": random.random() < 0.05,
            "metadata": {},
        })
    return out

def push_stub():
    requests.post(f"{API_BASE}/ingest/events", json=fetch_stub_events(200), timeout=30).raise_for_status()