"""justhodl-equity-ftd v1.0.0 (ops 3513) — SEC CNS fails-to-deliver as a
graded alpha family.

Extends ignition's proven fetch (cnsfails{YYYYMM}{a|b}.zip, pipe rows
SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE) into a
dedicated engine: 6 half-month files of per-symbol history, $-value
fails, PEAK-DAY days-to-cover vs FMP 20d average volume, spike ratio vs
the trailing-file mean, ETF/fund exclusion, liquidity floors, and
schema-v2 "ftd-squeeze" UP [21,63] signals vs SPY through the fleet
grading loop (checker-v3). Twice-weekly Scheduler; skips cheaply when
the SEC hasn't published a new file.
"""
import io
import json
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3

from signals_emit import log_signal

VERSION = "1.0.1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/equity-ftd.json"
UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
FMP_KEY_ENV = "FMP_KEY"
S3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")

ETFISH = ("ETF", "ETN", " FUND", "TRUST", "INDEX", "ISHARES", "SPDR",
          "VANGUARD", "PROSHARES", "DIREXION")

FLOORS = {"price": 5.0, "avg20": 500_000, "usd": 5_000_000,
          "spike": 3.0, "dtc_peak": 0.5}
MIN_SPIKE_BASE = 20_000  # prior-mean shares; kills near-zero-denominator artifacts
MAX_SIGNALS = 8
CAND_VOL_CAP = 350


def rnd(v, n=2):
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def _fetch(url, timeout=50):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=timeout).read()


def half_tags(n=8):
    """Candidate tags newest-first: for each month back, 'b' then 'a'."""
    now = datetime.now(timezone.utc)
    out = []
    for k in range(n):
        d = now.replace(day=15) - timedelta(days=31 * k)
        ym = d.strftime("%Y%m")
        out += [ym + "b", ym + "a"]
    return out


def parse_file(raw):
    """zip bytes -> (per_sym, meta). per_sym[sym] = {q, usd, days:{d:q},
    desc}. Bad lines skipped; usd only when the price field parses."""
    zf = zipfile.ZipFile(io.BytesIO(raw))
    txt = zf.read(zf.namelist()[0]).decode("utf-8", "replace")
    per = {}
    dates = set()
    n = 0
    for ln in txt.split("\n")[1:]:
        c = ln.split("|")
        if len(c) < 5:
            continue
        sym = c[2].strip()
        if not sym:
            continue
        try:
            q = int(c[3])
        except ValueError:
            continue
        d = c[0].strip()
        n += 1
        dates.add(d)
        e = per.setdefault(sym, {"q": 0, "usd": 0.0, "days": {},
                                 "desc": (c[4] or "").strip()[:60]})
        e["q"] += q
        e["days"][d] = e["days"].get(d, 0) + q
        if len(c) >= 6:
            try:
                e["usd"] += q * float(c[5])
            except ValueError:
                pass
    return per, {"n_rows": n, "n_symbols": len(per),
                 "dates": sorted(dates)}


def fetch_history(max_files=6):
    files = []
    for tag in half_tags():
        if len(files) >= max_files:
            break
        url = ("https://www.sec.gov/files/data/fails-deliver-data/"
               f"cnsfails{tag}.zip")
        try:
            per, meta = parse_file(_fetch(url))
            files.append({"tag": tag, "per": per, "meta": meta})
            print(f"[ftd] {tag}: {meta['n_symbols']} syms "
                  f"{meta['n_rows']} rows")
        except Exception as e:  # noqa: BLE001
            print(f"[ftd] {tag}: {str(e)[:60]}")
        time.sleep(0.3)
    return files


def spike_map(files):
    """latest total vs mean of prior files' totals (>=3 priors)."""
    latest = files[0]["per"]
    priors = files[1:]
    out = {}
    for sym, e in latest.items():
        obs = [p["per"][sym]["q"] for p in priors if sym in p["per"]]
        if len(obs) >= 3:
            mu = sum(obs) / len(obs)
            if mu >= MIN_SPIKE_BASE:
                out[sym] = (e["q"] / mu, mu)
    return out


def is_etfish(desc):
    u = (desc or "").upper()
    return any(t in u for t in ETFISH)


def fmp_vol_price(sym, key):
    """20d avg volume + last close from FMP light; None on any gap."""
    try:
        frm = (datetime.now(timezone.utc).date()
               - timedelta(days=45)).isoformat()
        raw = json.loads(_fetch(
            "https://financialmodelingprep.com/stable/historical-price-eod/"
            f"light?symbol={sym}&from={frm}&apikey={key}", timeout=25))
        hist = raw if isinstance(raw, list) else \
            (raw.get("historical") or raw.get("data") or [])
        rows = sorted((str(r["date"])[:10], r.get("close") or r.get("price"),
                       r.get("volume")) for r in hist
                      if isinstance(r, dict) and r.get("date"))
        vols = [v for _, _, v in rows[-20:]
                if isinstance(v, (int, float)) and v > 0]
        px = next((c for _, c, _ in reversed(rows)
                   if isinstance(c, (int, float)) and c > 0), None)
        if len(vols) >= 15 and px:
            return sum(vols) / len(vols), float(px)
    except Exception:  # noqa: BLE001
        pass
    return None, None


def evaluate(files, vol_fn, floors=FLOORS, max_signals=MAX_SIGNALS):
    """Pure decision core (unit-tested): returns (rows, qualifiers).
    rows = enriched candidate list; qualifiers pass every floor."""
    latest = files[0]["per"]
    spk = spike_map(files)
    by_usd = sorted(latest.items(), key=lambda x: -x[1]["usd"])[:250]
    by_spk = sorted(((s, v[0]) for s, v in spk.items()
                     if latest[s]["q"] >= 100_000),
                    key=lambda x: -x[1])[:250]
    cand = list(dict.fromkeys(
        [s for s, _ in by_usd] + [s for s, _ in by_spk]))[:CAND_VOL_CAP]
    rows = []
    for sym in cand:
        e = latest[sym]
        if is_etfish(e["desc"]):
            continue
        av, px = vol_fn(sym)
        peak = max(e["days"].values()) if e["days"] else 0
        r = {"t": sym, "q": e["q"], "usd": rnd(e["usd"] / 1e6, 2),
             "peak_day_q": peak,
             "spike": rnd(spk.get(sym, (None, None))[0], 2),
             "base": rnd(spk.get(sym, (None, None))[1], 0),
             "avg20": rnd(av, 0), "px": rnd(px, 2),
             "dtc_peak": rnd(peak / av, 2) if av else None,
             "desc": e["desc"][:40]}
        rows.append(r)
    quals = [r for r in rows
             if (r["px"] or 0) >= floors["price"]
             and (r["avg20"] or 0) >= floors["avg20"]
             and (r["usd"] or 0) * 1e6 >= floors["usd"]
             and (r["spike"] or 0) >= floors["spike"]
             and (r["dtc_peak"] or 0) >= floors["dtc_peak"]]
    quals.sort(key=lambda r: -(r["spike"] or 0))
    return rows, quals[:max_signals]


def lambda_handler(event, context):
    t0 = time.time()
    event = event or {}
    import os
    key = os.environ.get(FMP_KEY_ENV, "")
    prev_tag = None
    try:
        prev = json.loads(S3.get_object(Bucket=BUCKET, Key=KEY)
                          ["Body"].read())
        prev_tag = (prev.get("files") or [None])[0]
    except Exception:  # noqa: BLE001
        pass
    files = fetch_history()
    if not files:
        return {"ok": False, "error": "no SEC files reachable"}
    if files[0]["tag"] == prev_tag and not event.get("force"):
        out = {"ok": True, "version": VERSION, "unchanged": True,
               "latest_tag": prev_tag,
               "generated_at": datetime.now(timezone.utc).isoformat()}
        print(json.dumps(out))
        return out
    rows, quals = evaluate(files, lambda s: fmp_vol_price(s, key))
    logged, sigs = 0, []
    tbl = ddb.Table("justhodl-signals")
    for r in quals:
        ok = log_signal(
            tbl, "ftd-squeeze", r["t"], "UP", [21, 63], r["px"],
            confidence=min(0.75, 0.55 + (r["spike"] or 0) / 30.0),
            rationale=(f"FTD spike {r['spike']}x vs trailing mean; "
                       f"${r['usd']}M fails, peak-day DTC "
                       f"{r['dtc_peak']} vs 20d volume"),
            benchmark="SPY",
            metadata={"engine": "equity-ftd",
                      "tag": files[0]["tag"], "q": r["q"],
                      "usd_m": r["usd"], "dtc_peak": r["dtc_peak"],
                      "spike": r["spike"]})
        if ok:
            logged += 1
            sigs.append(r["t"])
    top_d = sorted([r for r in rows if r["usd"]],
                   key=lambda r: -r["usd"])[:30]
    top_s = sorted([r for r in rows if r["spike"]],
                   key=lambda r: -r["spike"])[:30]
    doc = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": rnd(time.time() - t0, 1),
           "files": [f["tag"] for f in files],
           "latest_tag": files[0]["tag"],
           "latest_meta": files[0]["meta"],
           "universe_n": files[0]["meta"]["n_symbols"],
           "n_candidates": len(rows),
           "floors": FLOORS,
           "top_dollars": top_d, "top_spikes": top_s,
           "qualifiers": quals, "signals": sigs, "logged": logged}
    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({k: doc[k] for k in
                      ("files", "universe_n", "n_candidates",
                       "signals", "logged")}))
    return doc
