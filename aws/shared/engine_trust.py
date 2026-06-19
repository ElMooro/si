"""engine_trust — fleet trust registry reader (the auto-demotion gate, consumer side).

Any engine/consumer imports this to down-weight signals from engines the truth layer
has shown to be below their null benchmark, conditioned on the CURRENT regime. Reads
data/engine-trust.json (produced by justhodl-engine-trust from the signal-scorecard).
Defaults to 1.0 (neutral) when the registry is missing or the engine is still WARMING —
so it is a no-op until the ledger matures, then bites automatically.
"""
import json
import time

import boto3

_S3 = boto3.client("s3", "us-east-1")
_BUCKET = "justhodl-dashboard-live"
_KEY = "data/engine-trust.json"
_CACHE = {"t": 0.0, "m": {}}


def _load():
    if time.time() - _CACHE["t"] < 300 and _CACHE["m"]:
        return _CACHE["m"]
    try:
        d = json.loads(_S3.get_object(Bucket=_BUCKET, Key=_KEY)["Body"].read())
        _CACHE["m"] = {e["signal_type"]: e for e in d.get("engines", [])}
        _CACHE["t"] = time.time()
    except Exception:
        pass
    return _CACHE["m"]


def trust(signal_type, default=1.0):
    """Effective regime-conditioned trust multiplier for an engine/signal_type."""
    e = _load().get(signal_type)
    if not e:
        return default
    v = e.get("effective_trust")
    return float(v) if v is not None else default


def status(signal_type):
    e = _load().get(signal_type)
    return e.get("status") if e else "UNKNOWN"
