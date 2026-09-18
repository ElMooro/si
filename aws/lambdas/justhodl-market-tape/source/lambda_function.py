"""Market tape v2: identified quotes, dated official observations and archived evidence.

The Fed broad trade-weighted dollar index is USD BROAD, not ICE DXY. The FMP
^IXIC quote is Nasdaq Composite (COMP), not Nasdaq-100 (NDX). Economic series
retain their definitions and observation clocks through the indicator bus.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone

import boto3
from evidence_store import capture
from macro_observations import finite, valid_yoy_row
from managed_secret import managed_secret

BUCKET = os.environ.get("JH_BUCKET", "justhodl-dashboard-live")
KEY = "data/market-tape.json"
_s3 = boto3.client("s3")
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ('/justhodl/fmp/api-key',))
FRED_KEY = managed_secret(('FRED_KEY', 'FRED_API_KEY'), ('/justhodl/fred/api-key',))


def source_json(url, provider):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-market-tape/2.0"})
    with urllib.request.urlopen(req, timeout=10) as response:
        raw = response.read()
    payload = json.loads(raw)
    evidence = capture(_s3, BUCKET, provider, url, raw)
    return payload, evidence


def fmp_quote(symbol, now):
    if not FMP_KEY:
        raise ValueError("quote provider unavailable")
    payload, evidence = source_json("https://financialmodelingprep.com/stable/quote?" +
        urllib.parse.urlencode({"symbol": symbol, "apikey": FMP_KEY}), "fmp")
    rows = payload if isinstance(payload, list) else [payload]
    matches = [r for r in rows if isinstance(r, dict) and r.get("symbol") == symbol]
    if len(matches) != 1:
        raise ValueError("quote identity mismatch")
    row = matches[0]
    value, stamp = finite(row.get("price")), finite(row.get("timestamp"))
    if value is None or value <= 0 or stamp is None:
        raise ValueError("quote price or observation time unavailable")
    observed = datetime.fromtimestamp(stamp, timezone.utc)
    age = (now - observed).total_seconds()
    if age < -300 or age > 7 * 86400:
        raise ValueError("quote is stale or future dated")
    return {"value": value, "chg_pct": finite(row.get("changesPercentage", row.get("changePercentage"))),
            "change_basis": "provider_previous_close", "observation_date": observed.date().isoformat(),
            "observed_at": observed.isoformat(), "received_at": evidence["first_received_at"],
            "provider_symbol": symbol, "source": "FMP", "src": "FMP:" + symbol,
            "definition": row.get("name") or symbol, "evidence": {"quote": evidence},
            "quality": {"status": "fresh" if age <= 900 else "delayed", "age_seconds": max(0, int(age)),
                        "basis": "provider_timestamp; exchange real-time entitlement not asserted"},
            "sizing_eligible": False}


def fred_latest(sid, now):
    if not FRED_KEY:
        raise ValueError("FRED provider unavailable")
    payload, evidence = source_json("https://api.stlouisfed.org/fred/series/observations?" +
        urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY, "file_type": "json",
                                "sort_order": "desc", "limit": 10}), "fred")
    rows = sorted(payload.get("observations") or [], key=lambda r: r["date"], reverse=True)
    valid = [(r, finite(r.get("value"))) for r in rows if finite(r.get("value")) is not None]
    if not valid:
        raise ValueError("no reported daily observation")
    row, value = valid[0]
    observed = date.fromisoformat(row["date"])
    age = (now.date() - observed).days
    max_age = 11 if sid == "DTWEXBGS" else 7
    if not 0 <= age <= max_age:
        raise ValueError("daily observation is stale or future dated")
    previous = valid[1] if len(valid) > 1 else None
    change = ((value / previous[1] - 1) * 100) if previous and previous[1] != 0 else None
    return {"value": value, "chg_pct": round(change, 6) if change is not None else None,
            "change_basis": "previous_reported_observation", "comparison_date": previous[0]["date"] if previous else None,
            "observation_date": observed.isoformat(), "asof": observed.isoformat(), "published_at": None,
            "source": "FRED", "src": "FRED:" + sid, "series_id": sid,
            "source_url": "https://fred.stlouisfed.org/series/" + sid,
            "received_at": evidence["first_received_at"], "evidence": {"observations": evidence},
            "quality": {"status": "fresh", "observation_age_days": age, "max_observation_age_days": max_age,
                        "basis": ("daily observations published weekly in H.10; not an intraday quote" if sid == "DTWEXBGS" else "daily published observation, not an intraday quote"),
                        "latest_returned_period": rows[0]["date"]}, "sizing_eligible": False}


def fmt(value, big=False):
    return f"{value:,.0f}" if big else (f"{value:,.1f}" if value >= 100 else f"{value:,.2f}")


def build_tape(now):
    items, gaps = [], []
    # Legacy labels are recorded explicitly so downstream migrations are visible.
    quotes = (("SPX", "^GSPC", False, "index_points", "S&P 500", None),
              ("COMP", "^IXIC", True, "index_points", "Nasdaq Composite", "NDX"),
              ("BTC", "BTCUSD", True, "USD_per_BTC", "Bitcoin / US dollar", None),
              ("GOLD", "GCUSD", True, "USD_per_troy_ounce", "Gold futures: provider generic contract", None))
    for label, symbol, big, unit, definition, old_label in quotes:
        try:
            item = fmp_quote(symbol, now)
            item.update(label=label, display=fmt(item["value"], big), unit=unit, definition=definition,
                        legacy_label=old_label, frequency="quote")
            items.append(item)
        except Exception as exc:
            gaps.append({"label": label, "reason": str(exc) if isinstance(exc, ValueError) else type(exc).__name__})
    daily = (("US10Y", "DGS10", "percent", "US Treasury 10-year constant maturity yield", None),
             ("VIX", "VIXCLS", "index_points", "CBOE VIX daily close", None),
             ("USD BROAD", "DTWEXBGS", "index_Jan2006_100", "Federal Reserve nominal broad US dollar index", "DXY"))
    for label, sid, unit, definition, old_label in daily:
        try:
            item = fred_latest(sid, now)
            item.update(label=label, unit=unit, definition=definition, legacy_label=old_label,
                        frequency="D", display=(f'{item["value"]:.2f}%' if unit == "percent" else f'{item["value"]:.1f}'))
            if label != "USD BROAD": item["chg_pct"] = None
            items.append(item)
        except Exception as exc:
            gaps.append({"label": label, "reason": str(exc) if isinstance(exc, ValueError) else type(exc).__name__})
    bus_stamp, bus_hash = None, None
    try:
        import hashlib
        raw = _s3.get_object(Bucket=BUCKET, Key="data/indicator-bus.json")["Body"].read()
        bus_doc = json.loads(raw)
        bus_hash = hashlib.sha256(raw).hexdigest()
        bus_stamp = bus_doc.get("generated_at")
        bus_time = datetime.fromisoformat(str(bus_stamp).replace("Z", "+00:00"))
        if bus_time.tzinfo is None or not -300 <= (now - bus_time).total_seconds() <= 30 * 3600:
            raise ValueError("indicator bus publication stale or time unknown")
        bus = bus_doc.get("indicators") or {}
        for symbol, label in (("CNGDPYY", "CN GDP nominal YoY"), ("USIRYY", "US CPI SA YoY")):
            row = bus.get(symbol) or {}
            if not valid_yoy_row(row, symbol, now):
                gap = (bus_doc.get("gaps") or {}).get(symbol) or {}
                gaps.append({"label": label, "symbol": symbol, "reason": "current verified definition unavailable",
                             "last_observation_date": gap.get("observation_date") or row.get("observation_date")})
                continue
            item = dict(row)
            item.update(label=label, value=row["v"], display=f'{row["v"]:.2f}%', symbol=symbol,
                        source_packet="data/indicator-bus.json", source_packet_sha256=bus_hash)
            items.append(item)
        # DE10Y previously accepted unitless/stale rows. Keep the input discoverable,
        # but publish it only when its own definition, date and percent unit are known.
        row = bus.get("DE10Y") or {}
        try:
            day = date.fromisoformat(row["observation_date"])
            if (finite(row.get("v")) is None or row.get("unit") not in ("%", "percent")
                    or not 0 <= (now.date() - day).days <= 7 or not row.get("evidence")
                    or row.get("quality", {}).get("status") != "fresh"):
                raise ValueError("DE10Y definition unavailable")
            items.append({**row, "label": "DE10Y", "value": row["v"], "display": f'{row["v"]:.2f}%',
                          "source_packet_sha256": bus_hash, "sizing_eligible": False})
        except (ValueError, KeyError, TypeError):
            gaps.append({"label": "DE10Y", "reason": "dated percent-yield evidence unavailable"})
    except Exception as exc:
        gaps.append({"label": "macro inputs", "reason": str(exc) if isinstance(exc, ValueError) else type(exc).__name__})
    generated = now.isoformat()
    return {"schema_version": "2.0", "generated": generated, "generated_at": generated,
            "items": items, "n": len(items), "gaps": gaps, "_data_quality": "identified_and_dated",
            "quality": {"status": "partial" if gaps else "fresh", "missing_count": len(gaps)},
            "bus_tape": {"marker": "macro-contract.v1", "source_generated_at": bus_stamp,
                         "source_sha256": bus_hash, "added": [i["label"] for i in items if i.get("symbol")]},
            "sizing_eligible": False}


def lambda_handler(event=None, context=None):
    packet = build_tape(datetime.now(timezone.utc))
    _s3.put_object(Bucket=BUCKET, Key=KEY, Body=json.dumps(packet, allow_nan=False).encode(),
                   ContentType="application/json", CacheControl="max-age=120")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n": packet["n"],
            "labels": [item["label"] for item in packet["items"]], "gaps": packet["gaps"]})}
