# path: tests/test_api_ingest.py
from __future__ import annotations
import pytest


@pytest.mark.asyncio
async def test_ingest_and_query_roundtrip(client):
    events = [{
        "ts": "2025-01-01T00:00:00Z",
        "source": "test",
        "event_type": "auth_failure",
        "user_id": "u1",
        "account_id": "a1",
        "device_id": "d1",
        "ip": "203.0.113.10",
        "geo_lat": 48.85,
        "geo_lon": 2.35,
        "country": "FR",
        "city": "Paris",
        "is_unusual": False,
        "metadata": {},
    }]
    r = await client.post("/ingest/events", json=events)
    assert r.status_code == 200

    r = await client.get("/query/events", params={"user": "u1"})
    assert r.status_code == 200
    body = r.json()
    assert body["items"] and body["items"][0]["user_id"] == "u1"