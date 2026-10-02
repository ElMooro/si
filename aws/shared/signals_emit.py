"""Shared signal-event writer (legacy schema-v2).

Fabric diagnostics are retained separately as unqualified research context.
An emitted event is not proof of gradeability, source timing or forecast skill.
Further core event-contract repairs are tracked in the implementation ledger.

Legacy storage fields retained for compatibility:
  measure_against = the actual SYMBOL to price
  check_windows   = ["5","21",…]  AND  check_timestamps = {"day_5": iso,…}
  baseline_price positive and typed; yprice() is a legacy quote helper, not entry-mark evidence
  dedupe via ConditionExpression on signal_id = f"{type}#{TICKER}#{date}"
"""

import json
from signal_event_inputs import prepare as prepare_event_inputs, exact as exact_event_values
from fabric_logging_context import read as read_fabric_research, select as select_fabric_research, separate as separate_fabric_metadata
import boto3
import time
import re
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal

_UA = {"User-Agent": "Mozilla/5.0 (JustHodl-fleet)"}


def yprice(sym):
    """Latest close, Yahoo v8 keyless. None on any failure."""
    try:
        u = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
             "?range=5d&interval=1d")
        with urllib.request.urlopen(urllib.request.Request(u, headers=_UA), timeout=12) as r:
            j = json.loads(r.read())
        res = j["chart"]["result"][0]
        m = res.get("meta") or {}
        p = m.get("regularMarketPrice")
        if p:
            return float(p)
        cl = [c for c in res["indicators"]["quote"][0]["close"] if c]
        return float(cl[-1]) if cl else None
    except Exception:
        return None


def _f2d(x):
    return exact_event_values(x)


_REGIME = {"t": 0.0, "v": None}


_SUP = {"t": 0.0, "v": None}


def _suppress_set():
    """ops 3426 — alpha-triage RETIRE list: families proven noise stop
    emitting fleet-wide. Cached 10 min; empty set on any failure."""
    if time.time() - _SUP["t"] < 600 and _SUP["v"] is not None:
        return _SUP["v"]
    out = set()
    try:
        s3c = boto3.client("s3", "us-east-1")
        j = json.loads(s3c.get_object(Bucket="justhodl-dashboard-live",
                                      Key="data/signal-suppress.json")["Body"].read())
        out = set(j.get("suppressed") or [])
    except Exception:
        pass
    _SUP["v"] = out
    _SUP["t"] = time.time()
    return out


def _regime_snapshot():
    """ops 3412 — regime stamp at emission. Every signal carries the regime it
    fired in (JSI decile, GSSI band, liquidity), so grading becomes natively
    per-regime across the whole fleet. Cached 5 min; never raises."""
    if time.time() - _REGIME["t"] < 300 and _REGIME["v"] is not None:
        return _REGIME["v"]
    out = {}
    try:
        s3c = boto3.client("s3", "us-east-1")

        def _rj(k):
            try:
                return json.loads(s3c.get_object(
                    Bucket="justhodl-dashboard-live", Key=k)["Body"].read())
            except Exception:
                return {}
        j = _rj("data/stress-index.json")
        lat = (j.get("latest") or j.get("v2") or {})
        dec = (lat.get("decile") if isinstance(lat.get("decile"), int)
               else (j.get("signal_state") or {}).get("decile"))
        if dec is None:
            pv = lat.get("pctile") or lat.get("percentile")
            dec = int(min(9, float(pv) // 10)) if pv is not None else None
        if dec is not None:
            out["jsi_decile"] = int(dec)
        g = (_rj("data/sovereign-gssi.json").get("latest") or {}).get("gssi")
        if g is not None:
            out["gssi_band"] = ("CRISIS" if g >= 75 else "STRESS" if g >= 60
                                else "ELEVATED" if g >= 45 else "NORMAL"
                                if g >= 30 else "CALM")
        rm = _rj("data/regime-map.json")
        lab = ((rm.get("regime") or {}).get("label")
               if isinstance(rm.get("regime"), dict) else rm.get("regime"))
        if lab:
            out["label"] = str(lab)[:32]
        li = _rj("data/liquidity-inflection.json")
        tone = ((li.get("composite") or {}).get("tone") or li.get("tone")
                or (li.get("onshore_funding") or {}).get("tone"))
        if tone:
            out["liquidity"] = str(tone)[:16]
    except Exception:
        pass
    try:
        sm = _rj("data/spx-ma.json")
        ix = sm.get("index") or {}
        br = sm.get("breadth") or {}
        if ix.get("regime"):
            out["spx_regime"] = str(ix["regime"])[:12]
            if ix.get("stack") is not None:
                out["spx_stack"] = str(ix.get("stack"))[:14]
        b2 = br.get("pct_above_200d") or br.get("200d")
        if isinstance(b2, (int, float)):
            out["breadth200"] = round(float(b2), 1)
        nm = (sm.get("divergence") or {}).get("narrow_market", sm.get("narrow_market"))
        if nm is not None:
            out["narrow_market"] = bool(nm)
        fv = _rj("data/fifx-vol.json")
        mg = fv.get("migration") or {}
        if mg.get("state"):
            out["vol_state"] = str(mg["state"])[:18]
        if mg.get("asia_state"):
            out["asia_vol"] = str(mg["asia_state"])[:14]
        gb = (fv.get("global") or {}).get("breadth_pct")
        if isinstance(gb, (int, float)):
            out["gvol_breadth"] = round(float(gb), 1)
        kt = ((_rj("data/asia-leads.json").get("korea_flash_tape") or {}).get("latest") or {})
        if isinstance(kt.get("yoy_pct"), (int, float)):
            out["kr_flash_yoy"] = kt["yoy_pct"]
    except Exception:
        pass
    _REGIME["v"] = out
    _REGIME["t"] = time.time()
    return out



_FB_CACHE = {"t": None, "d": None}


def _fabric_ctx(sym):
    """Selected diagnostics only; cache age never establishes source freshness."""
    stamp = time.monotonic()
    prior = _FB_CACHE.get("t")
    age = stamp - prior if type(prior) in (int, float) else None
    if _FB_CACHE.get("d") is None or age is None or age < 0 or age > 900:
        try:
            snapshot = read_fabric_research(boto3.client("s3", region_name="us-east-1"),
                                            "justhodl-dashboard-live")
        except Exception:
            snapshot = None
        # A failed refresh replaces the earlier cached context; no last-good
        # values can silently masquerade as this read.
        _FB_CACHE.update(t=time.monotonic(), d=snapshot)
        age = 0
    result = select_fabric_research(_FB_CACHE["d"], sym)
    result["cache_age_s"] = round(age, 3)
    return result


def log_signal(table, signal_type, ticker, direction, windows, baseline_price,
               confidence=0.55, rationale="", metadata=None, benchmark=None,
               signal_value=""):
    """Write a validated research event in legacy schema-v2.
    Returns False on invalid inputs, dedupe or storage failure. Validation does
    not qualify entry marks or forward performance. `table` is a DynamoDB Table.
    """
    try:
        prepared = prepare_event_inputs(signal_type, ticker, direction, windows,
                                        baseline_price, confidence, metadata, benchmark)
    except Exception:
        return False
    direction = prepared['direction']; windows = prepared['windows']
    baseline_price = prepared['baseline_price']; confidence = prepared['confidence']
    metadata = prepared['metadata']
    if signal_type in _suppress_set():
        print(f"[signals] SUPPRESSED family {signal_type} (alpha-triage RETIRE)")
        return False
    md = dict(metadata or {})
    md.setdefault("regime", _regime_snapshot())
    metadata = md
    now = datetime.now(timezone.utc)
    try:
        # Strip caller and SDK legacy learning keys even when the feed fails.
        # Their values remain separate inspectable, explicitly unqualified context.
        metadata = separate_fabric_metadata(metadata, _fabric_ctx(ticker))
    except Exception:
        return False  # Never fall back to metadata that still grants false authority.
    try:
        item = {
            "signal_id": f"{signal_type}#{ticker}#{now.date().isoformat()}",
            "signal_type": signal_type,
            "signal_value": str(signal_value)[:40],
            "predicted_direction": direction,
            "confidence": confidence,
            "measure_against": ticker,
            "baseline_price": baseline_price,
            "baseline_benchmark_price": None,
            "benchmark": benchmark,
            "check_windows": [str(d) for d in windows],
            "check_timestamps": {f"day_{d}": (now + timedelta(days=d)).isoformat()
                                 for d in windows},
            "outcomes": {}, "accuracy_scores": {},
            "logged_at": now.isoformat(), "logged_epoch": int(now.timestamp()),
            "status": "pending", "schema_version": "2",
            "horizon_days_primary": max(windows),
            "ttl": int((now + timedelta(days=365)).timestamp()),
            "rationale": str(rationale)[:300],
            "metadata": _f2d(metadata or {}),
        }
    except Exception:
        return False
    try:
        table.put_item(Item=item,
                       ConditionExpression="attribute_not_exists(signal_id)")
        return True
    except Exception:
        return False
