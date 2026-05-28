"""jhcore.fred — FRED API client with retry, multi-series fetch, and history downsample."""
import json
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

DEFAULT_KEY = "2f057499936072679d8843d7fce99989"
BASE = "https://api.stlouisfed.org/fred"
UA = "JustHodl/jhcore"


def _key():
    return os.environ.get("FRED_KEY", DEFAULT_KEY)


def latest(series_id, key=None):
    """Most recent observation: returns (date_str, float) or None."""
    qs = urllib.parse.urlencode({
        "series_id": series_id, "api_key": key or _key(), "file_type": "json",
        "sort_order": "desc", "limit": 1,
    })
    url = f"{BASE}/series/observations?{qs}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
            for o in data.get("observations", []):
                v = o.get("value")
                if v not in (".", "", None):
                    try:
                        return (o["date"], float(v))
                    except ValueError:
                        return None
            return None
        except Exception:
            if attempt == 2:
                return None
            time.sleep(0.6 * (attempt + 1))


def history(series_id, start="1990-01-01", key=None):
    """Full history as sorted [(date_str, float), ...]."""
    qs = urllib.parse.urlencode({
        "series_id": series_id, "api_key": key or _key(), "file_type": "json",
        "observation_start": start,
    })
    url = f"{BASE}/series/observations?{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
        out = []
        for o in data.get("observations", []):
            v = o.get("value")
            if v not in (".", "", None):
                try:
                    out.append((o["date"], float(v)))
                except ValueError:
                    pass
        return out
    except Exception:
        return []


def batch_latest(series_ids, key=None, max_workers=8):
    """Fetch latest for many series in parallel: returns {sid: (date, value)} (skips failures)."""
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(latest, sid, key): sid for sid in series_ids}
        for f in as_completed(futures):
            sid = futures[f]
            try:
                r = f.result()
                if r:
                    out[sid] = r
            except Exception:
                pass
    return out


def value_at(hist, target_date, window_days=10):
    """Nearest observation to target_date in a history list. Returns value or None."""
    if not hist:
        return None
    tgt = datetime.strptime(target_date, "%Y-%m-%d").date()
    best, best_dist = None, 10**9
    for ds, v in hist:
        d = datetime.strptime(ds, "%Y-%m-%d").date()
        dist = abs((d - tgt).days)
        if dist < best_dist:
            best_dist = dist
            best = v
    if best_dist <= window_days:
        return best
    return best if best_dist <= 35 else None
