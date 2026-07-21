"""aws/shared/signals_emit.py — the ONE correct way to log a gradeable signal (ops 3379).

Fleet audit found ~40 direct emitters writing schema-v2 rows the outcome-
checker cannot score: no check_timestamps (its window loop no-ops) and/or a
LITERAL string in measure_against ("ticker", "ticker_vs_benchmark",
"ticker_vs_acwx"…) which the checker then tries to PRICE. The harvester is
the proven-correct template; this module is that template, shared.

Contract (mirrors justhodl-signal-harvester exactly):
  measure_against = the actual SYMBOL to price
  check_windows   = ["5","21",…]  AND  check_timestamps = {"day_5": iso,…}
  baseline_price REQUIRED (unscoreable otherwise) — yprice() included
  dedupe via ConditionExpression on signal_id = f"{type}#{TICKER}#{date}"
"""

import json
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
    if isinstance(x, float):
        return Decimal(str(round(x, 6)))
    if isinstance(x, dict):
        return {k: _f2d(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_f2d(v) for v in x]
    return x


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


def log_signal(table, signal_type, ticker, direction, windows, baseline_price,
               confidence=0.55, rationale="", metadata=None, benchmark=None,
               signal_value=""):
    """Write one harvester-contract row. Returns True if written, False on
    dedupe or bad inputs. `table` = boto3 dynamodb Table resource."""
    if not (ticker and re.fullmatch(r"[A-Z0-9.\-\^=]{1,10}", ticker)):
        return False
    if not baseline_price or baseline_price <= 0:
        return False
    if signal_type in _suppress_set():
        print(f"[signals] SUPPRESSED family {signal_type} (alpha-triage RETIRE)")
        return False
    md = dict(metadata or {})
    md.setdefault("regime", _regime_snapshot())
    metadata = md
    now = datetime.now(timezone.utc)
    windows = [int(w) for w in windows]
    item = {
        "signal_id": f"{signal_type}#{ticker}#{now.date().isoformat()}",
        "signal_type": signal_type,
        "signal_value": str(signal_value)[:40],
        "predicted_direction": direction,
        "confidence": _f2d(max(0.05, min(0.95, float(confidence)))),
        "measure_against": ticker,
        "baseline_price": _f2d(float(baseline_price)),
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
    try:
        table.put_item(Item=item,
                       ConditionExpression="attribute_not_exists(signal_id)")
        return True
    except Exception:
        return False
