# path: tests/test_mapping.py
from __future__ import annotations
import pandas as pd
from mapping.ai_mapper import suggest_event_mapping

def test_heuristic_mapping():
    df = pd.DataFrame({"timestamp":[1], "etype":["x"], "user":[1]})
    m = suggest_event_mapping(df, engine="heuristic")
    assert "ts" in m and m["ts"] in df.columns
    assert m["event_type"] in df.columns
