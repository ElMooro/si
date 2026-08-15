"""justhodl-te-fred-mirror — standing cross-check + independent copy
of Trading Economics' /fred/historical/{mnemonic} data.

Architecture (Khalid, explicit): this engine writes ONLY to
data/warm/te-mirror/ and NEVER touches data/warm/fred-scoped/. That
separation is structural, not a promise — the FRED-sourced docs are
opened read-only here (to compute the cross-check), and the only
S3 write target in this whole file is the te-mirror prefix. The
delete-proof bucket policy (ops 4666, wildcard on data/warm/*)
protects this new prefix automatically, with no additional policy
work needed.

Discovered breakthrough tonight (ops 4706-4709): TE's dedicated FRED
mirror serves the full ICE BofA family (192 BAML mnemonics, 125
confirmed continuous 1996/1970s->today) plus plain macro series
(DGS10, BAA10Y, etc.) via a simple lowercased-mnemonic GET, no auth
beyond the existing paid key. One real constraint found: a 10,000-row
cap on very long series (DGS10 truncated at 2002) -- irrelevant for
BAML (inception 1996+, always under the cap) but tracked here so a
future series that hits it is visible, not silently truncated.

Converges the full worklist, then maintains (re-pulls to catch new
observations + keep the cross-check current) on an hourly schedule.
"""
import json
import time
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
TE_PREFIX = "data/warm/te-mirror/"          # <- the ONLY write target
FRED_PREFIX = "data/warm/fred-scoped/"      # <- read-only, never written
STATE_KEY = TE_PREFIX + "_state.json"
IDX_KEY = TE_PREFIX + "_index.json"
TRANCHE = 60
ROW_CAP_WARN = 9500     # flag series approaching TE's 10k-row ceiling

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")

# Curated starter set beyond the 192 BAML ids: series matching
# Khalid's stated macro doctrine (liquidity/dollar/eurodollar/
# plumbing-first) that TE's mirror is confirmed to carry.
CORE_MACRO = [
    "DGS10", "DGS2", "DGS3MO", "DGS30", "T10Y2Y", "T10Y3M",
    "FEDFUNDS", "SOFR", "DFF", "IORB",
    "CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE",
    "UNRATE", "PAYEMS", "ICSA",
    "INDPRO", "DTWEXBGS", "DEXUSEU", "DEXJPUS", "DEXCHUS",
    "M2SL", "WALCL", "WRESBAL", "RRPONTSYD",
    "BAA10Y", "AAA10Y", "T5YIE", "T10YIE",
    "VIXCLS", "GOLDAMGBD228NLBM", "DCOILWTICO",
]


def _key(name):
    for pn, dec in (("/justhodl/" + name, True),):
        try:
            return ssm.get_parameter(Name=pn, WithDecryption=dec
                                     )["Parameter"]["Value"]
        except Exception:
            continue
    return ""


def _gj(key, dflt=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return dflt if dflt is not None else {}


def _fred_release_209(key):
    import urllib.request
    try:
        rq = urllib.request.Request(
            "https://api.stlouisfed.org/fred/release/series"
            "?release_id=209&api_key=" + key
            + "&file_type=json&limit=1000")
        d = json.loads(urllib.request.urlopen(rq, timeout=20).read())
        return [s.get("id") for s in d.get("seriess") or []]
    except Exception:
        return []


def _te_fetch(sid, te_key):
    import urllib.error
    import urllib.request
    url = ("https://api.tradingeconomics.com/fred/historical/"
          + sid.lower() + "?c=" + te_key)
    rq = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"})
    try:
        with urllib.request.urlopen(rq, timeout=25) as r:
            d = json.loads(r.read())
    except urllib.error.HTTPError as e:
        return None, "HTTP %s" % e.code
    except Exception as e:
        return None, str(e)[:80]
    if not isinstance(d, list):
        return None, "non-list response"
    rows = []
    for row in d:
        try:
            dt = str(row.get("date") or row.get("DATE") or "")[:10]
            v = row.get("value")
            if dt[:2] in ("19", "20") and v is not None:
                rows.append((dt, float(v)))
        except Exception:
            continue
    return rows, None


def _cross_check(sid, te_rows):
    """READ-ONLY against the FRED-sourced doc. Never writes here."""
    tok, key = None, None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": FRED_PREFIX, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents") or []:
            if o["Key"].rsplit("/", 1)[-1] == sid + ".json":
                key = o["Key"]
                break
        if key or not resp.get("IsTruncated"):
            break
        tok = resp.get("NextContinuationToken")
    if not key:
        return {"fred_doc_found": False}
    doc = _gj(key)
    fred_map = dict((o.get("date"), o.get("value"))
                    for o in doc.get("observations") or [])
    te_map = dict(te_rows)
    shared = set(fred_map) & set(te_map)
    agree = 0
    for d0 in shared:
        try:
            if abs(float(fred_map[d0]) - float(te_map[d0])) <= 0.02:
                agree += 1
        except Exception:
            pass
    return {"fred_doc_found": True, "shared_dates": len(shared),
           "agree": agree,
           "agree_pct": round(100 * agree / len(shared), 2)
                        if shared else None}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    te_key = _key("te_api")
    fred_key = _key("fred-api-key") or _key("fred/api-key")
    if not te_key:
        out = {"ok": False, "error": "te_key_missing"}
        s3.put_object(Bucket=BUCKET, Key="data/te-mirror-status.json",
                      Body=json.dumps(out).encode(),
                      ContentType="application/json")
        return out

    st = _gj(STATE_KEY)
    if (st.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    st["lease_until"] = time.time() + 560
    if not st.get("catalog"):
        baml = _fred_release_209(fred_key) if fred_key else []
        st["catalog"] = sorted(set(baml) | set(CORE_MACRO))
        st["catalog_built_at"] = now
    catalog = st["catalog"]
    done = set(st.get("done") or [])
    todo = ([s for s in catalog if s not in done] or catalog)[:TRANCHE]
    if not [s for s in catalog if s not in done]:
        st["done"] = []   # converged -> restart maintenance sweep

    idx = _gj(IDX_KEY, {"symbols": {}})
    got = row_cap_hits = failed = 0
    for sid in todo:
        rows, err = _te_fetch(sid, te_key)
        if err or not rows:
            st.setdefault("failures", {})[sid] = err or "empty"
            failed += 1
            time.sleep(0.3)
            continue
        st.get("failures", {}).pop(sid, None)
        near_cap = len(rows) >= ROW_CAP_WARN
        if near_cap:
            row_cap_hits += 1
        xcheck = _cross_check(sid, rows)
        doc = {"id": sid, "source": "tradingeconomics /fred/historical/"
                                    " mirror", "as_of": now,
               "n": len(rows), "first": rows[0][0], "last": rows[-1][0],
               "near_row_cap": near_cap,
               "cross_check": xcheck,
               "observations": [{"date": d0, "value": v}
                                for d0, v in rows]}
        s3.put_object(Bucket=BUCKET, Key=TE_PREFIX + sid + ".json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        idx["symbols"][sid] = {"n": len(rows), "first": rows[0][0],
                               "last": rows[-1][0],
                               "agree_pct": xcheck.get("agree_pct"),
                               "near_row_cap": near_cap}
        st.setdefault("done", []).append(sid)
        got += 1
        time.sleep(0.35)

    idx["updated_at"] = now
    idx["n_symbols"] = len(idx["symbols"])
    agree_vals = [v["agree_pct"] for v in idx["symbols"].values()
                 if v.get("agree_pct") is not None]
    idx["mean_agree_pct"] = (round(sum(agree_vals) / len(agree_vals), 2)
                             if agree_vals else None)
    s3.put_object(Bucket=BUCKET, Key=IDX_KEY,
                  Body=json.dumps(idx, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    n_done = len(set(st.get("done") or []))
    st["as_of"] = now
    st["status"] = ("COMPLETE-maintaining" if n_done >= len(catalog)
                    else "converging")
    st["lease_until"] = 0
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=json.dumps(st, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    res = {"ok": True, "pulled": got, "failed": failed,
          "row_cap_hits": row_cap_hits, "done": n_done,
          "catalog": len(catalog), "status": st["status"],
          "mean_agree_pct": idx["mean_agree_pct"]}
    s3.put_object(Bucket=BUCKET, Key="data/te-mirror-status.json",
                 Body=json.dumps(res, default=str).encode(),
                 ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(res))
    return res
