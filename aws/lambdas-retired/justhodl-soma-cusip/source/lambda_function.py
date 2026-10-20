"""justhodl-soma-cusip — security-level Fed holdings, the last NY Fed gap
(ops 4757). ops 4743 found 1,206 weekly as-of dates of per-CUSIP SOMA
holdings and flagged them NEEDS_DEDICATED_ENGINE — too big to one-shot,
exactly right for the ofr-stfm convergence pattern:

  every run: refresh the as-of date catalog (catches each new week),
  take the next TRANCHE undone (type, date) pairs, fetch, bank raw gz
  to data/warm/nyfed-markets/soma-cusip/{type}/{date}.json.gz
  (deny-Delete + versioned = permanent), advance state. Endpoint shape
  is VALIDATED at runtime from a candidate list (first candidate that
  returns rows wins and is remembered in state) — never assumed.

Converges ~1,206 dates x 2 types in ~10 tranches on the 3h schedule,
then costs pennies keeping up with one new date/week. Explicit
failures; hot status doc at data/soma-cusip-status.json."""
import gzip
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
NYF = "https://markets.newyorkfed.org"
STATE_KEY = "data/warm/nyfed-markets/soma-cusip/_state.json"
HOT_KEY = "data/soma-cusip-status.json"
TRANCHE = int(os.environ.get("SOMA_TRANCHE", "120"))
UA = {"User-Agent": "JustHodl research raafouis@gmail.com"}

TYPE_CANDS = {
    "tsy": ["/api/soma/tsy/get/asof/{d}.json",
             "/api/soma/tsy/get/all/asof/{d}.json"],
    "agency": ["/api/soma/agency/get/asof/{d}.json",
                "/api/soma/agency/get/all/asof/{d}.json",
                "/api/soma/agency/get/mbs/asof/{d}.json"],
}

s3 = boto3.client("s3", region_name="us-east-1")


def _fetch(u, timeout=90):
    req = urllib.request.Request(u, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    if b[:2] == b"\x1f\x8b":
        b = gzip.decompress(b)
    return b


def _rows(d):
    if isinstance(d, list):
        return d
    if isinstance(d, dict):
        for v in d.values():
            if isinstance(v, list) and v:
                return v
        for v in d.values():
            if isinstance(v, dict):
                for v2 in v.values():
                    if isinstance(v2, list) and v2:
                        return v2
    return []


def _load_state():
    try:
        return json.loads(gzip.decompress(
            s3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read()))
    except Exception:
        try:
            return json.loads(
                s3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
        except Exception:
            return {"asof": [], "done": {}, "endpoint": {}, "failed": {}}


def _save_state(st):
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                   Body=json.dumps(st, separators=(",", ":")).encode(),
                   ContentType="application/json")


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    st = _load_state()
    st.setdefault("done", {})
    st.setdefault("endpoint", {})
    st.setdefault("failed", {})
    S = {"as_of": now}

    # 1) refresh as-of catalog — catches each new weekly date forever
    try:
        d = json.loads(_fetch(f"{NYF}/api/soma/asofdates/list.json"))
        dates = sorted({m for m in re.findall(
            r"\b(20\d{2}-\d{2}-\d{2})\b", json.dumps(d))})
        if dates:
            st["asof"] = dates
        S["asof_catalog"] = len(st.get("asof") or [])
    except Exception as e:
        S["asof_catalog_error"] = f"{type(e).__name__}: {str(e)[:70]}"

    asof = st.get("asof") or []
    if not asof:
        S["fatal"] = "no as-of catalog available"
        s3.put_object(Bucket=BUCKET, Key=HOT_KEY,
                       Body=json.dumps(S).encode(),
                       ContentType="application/json", CacheControl="no-cache")
        return {"statusCode": 500, "body": json.dumps(S)}

    # 2) work newest-first so fresh weeks land immediately, backfill trails
    pending = []
    for typ in TYPE_CANDS:
        done_set = set(st["done"].get(typ) or [])
        fail_map = st["failed"].get(typ) or {}
        for dt in reversed(asof):
            if dt in done_set:
                continue
            if fail_map.get(dt, 0) >= 3:
                continue  # permanent-fail after 3 strikes; listed in state
            pending.append((typ, dt))
    S["pending_before"] = len(pending)

    banked = 0
    errs = 0
    for typ, dt in pending[:TRANCHE]:
        if (time.time() - t0) > 780:  # leave headroom under 900s timeout
            S["stopped_for_time"] = True
            break
        cands = ([st["endpoint"][typ]] if st["endpoint"].get(typ)
                  else []) + [c for c in TYPE_CANDS[typ]
                               if c != st["endpoint"].get(typ)]
        got = False
        for tpl in cands:
            try:
                raw = _fetch(NYF + tpl.format(d=dt))
                doc = json.loads(raw)
                rows = _rows(doc)
                if not rows:
                    continue
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"data/warm/nyfed-markets/soma-cusip/{typ}/{dt}.json.gz",
                    Body=gzip.compress(raw),
                    ContentType="application/json", ContentEncoding="gzip")
                st["done"].setdefault(typ, []).append(dt)
                st["endpoint"][typ] = tpl
                banked += 1
                got = True
                break
            except Exception:
                continue
        if not got:
            errs += 1
            fm = st["failed"].setdefault(typ, {})
            fm[dt] = fm.get(dt, 0) + 1
        if banked % 30 == 0 and banked:
            _save_state(st)
        time.sleep(0.12)

    for typ in st["done"]:
        st["done"][typ] = sorted(set(st["done"][typ]))
    st["updated_at"] = now
    _save_state(st)

    total_pairs = len(asof) * len(TYPE_CANDS)
    done_pairs = sum(len(v) for v in st["done"].values())
    S.update({"banked_this_run": banked, "errors_this_run": errs,
               "done_pairs": done_pairs, "total_pairs": total_pairs,
               "progress_pct": round(100.0 * done_pairs / max(1, total_pairs), 1),
               "endpoint": st["endpoint"],
               "permanent_fails": {k: len(v) for k, v in st["failed"].items()}})
    s3.put_object(Bucket=BUCKET, Key=HOT_KEY, Body=json.dumps(S).encode(),
                   ContentType="application/json", CacheControl="no-cache")
    print("[soma-cusip] " + json.dumps(S)[:400])
    return {"statusCode": 200, "body": json.dumps(S)}
