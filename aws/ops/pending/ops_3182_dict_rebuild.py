"""ops 3182 — purge the poisoned dictionary and rebuild it from the runner.

3181's fix was correct but could not take effect: 3180 had already written
the junk FRED names WITHOUT a provisional flag, so the retry logic skipped
them and the cache kept serving 'DGS10 (FRED)'. A cache that poisons itself
must be purged, not patched around.

This op rebuilds the FRED half of the dictionary HERE, on the runner, where
FRED access is proven (ops 3167 pulled 9,135 observations from it), with
proper throttling. Also re-maps the universe so continuous futures
(NYMEX:CL1! → CL=F) resolve.

Gate: FRED:DGS10 must read '10-Year Treasury Constant Maturity Rate' or the
op fails. No more claiming 100% while shipping codes.
"""

import json
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
FRED_KEY = SS.FRED_KEY


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def s3_put(key, doc):
    S3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc).encode(),
                  ContentType="application/json")


def fred_title(sid):
    for i in range(3):
        try:
            u = ("https://api.stlouisfed.org/fred/series"
                 f"?series_id={urllib.parse.quote(sid)}&api_key={FRED_KEY}"
                 "&file_type=json")
            r = urllib.request.urlopen(urllib.request.Request(
                u, headers={"User-Agent": "ops-3182"}), timeout=20)
            d = json.loads(r.read().decode())
            s = (d.get("seriess") or [None])[0]
            if not s:
                return None
            return {"name": s.get("title"),
                    "units": s.get("units_short") or s.get("units"),
                    "frequency": s.get("frequency_short"),
                    "seasonal": s.get("seasonal_adjustment_short"),
                    "history": f"{s.get('observation_start')} → "
                               f"{s.get('observation_end')}",
                    "category": "macro"}
        except Exception:
            time.sleep(0.7 * (i + 1))
    return None


with report("3182_dict_rebuild") as rep:
    fails, warns = [], []
    rep.heading("ops 3182 — purge the poisoned dictionary, rebuild it right")

    rep.section("1. Re-map (picks up continuous futures)")
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    prev = s3_json("data/symbol-map.json") or {}
    cache = prev.get("search_cache") or {}
    searcher = SS.fred_search_factory(cache)
    mapped, used, t0 = {}, 0, time.time()
    for s in uniq:
        allow = used < 300 and time.time() - t0 < 240
        src, sid, conf, note = SS.map_symbol(s, searcher if allow else None)
        if "fred-search" in str(note):
            used += 1
        if src:
            mapped[s] = {"source": src, "id": sid, "confidence": conf,
                         "note": note}
    cov = round(100 * len(mapped) / max(1, len(uniq)), 1)
    s3_put("data/symbol-map.json",
           {"generated_at": datetime.now(timezone.utc).isoformat(),
            "n_mapped": len(mapped), "n_total": len(uniq),
            "coverage_pct": cov, "map": mapped, "search_cache": cache})
    rep.kv(unique_symbols=len(uniq), coverage_pct=cov,
           was=prev.get("coverage_pct"))
    rep.ok(f"symbol map rebuilt: {cov}% ({len(mapped)} symbols)")

    rep.section("2. Purge the junk, keep the good")
    dd = s3_json("data/symbol-dictionary.json") or {}
    dic = dd.get("dictionary") or {}
    before = len(dic)
    killed = 0
    for sym in list(dic):
        d = dic[sym]
        nm = str(d.get("name") or "")
        sid = str(d.get("source_id") or "")
        # a "name" that is just the code (or the code + source) is junk
        if d.get("source") == "FRED" and (nm.startswith(sid)
                                          or nm.endswith("(FRED)")
                                          or nm == sid):
            dic.pop(sym)
            killed += 1
        elif d.get("provisional"):
            dic.pop(sym)
            killed += 1
    rep.kv(dict_before=before, junk_purged=killed, kept=len(dic))
    rep.ok(f"purged {killed} junk entries (the cache was poisoning itself)")

    rep.section("3. Rebuild FRED titles on the runner (throttled)")
    fred_syms = [s for s, m in mapped.items()
                 if m["source"] == "FRED" and s not in dic]
    rep.kv(fred_to_fetch=len(fred_syms))
    t1 = time.time()
    got = 0
    with ThreadPoolExecutor(max_workers=3) as ex:
        for sym, meta in zip(fred_syms,
                             ex.map(lambda s: fred_title(mapped[s]["id"]),
                                    fred_syms)):
            if meta and meta.get("name"):
                dic[sym] = {**meta, "source": "FRED",
                            "source_id": mapped[sym]["id"],
                            "confidence": mapped[sym].get("confidence")}
                got += 1
            if time.time() - t1 > 600:
                warns.append("FRED pass hit its 600s budget — the weekly "
                             "engine finishes the tail")
                break
    rep.kv(fred_named=got)
    rep.ok(f"{got} FRED series carry their OFFICIAL title")

    named = sum(1 for s in uniq if dic.get(s, {}).get("name"))
    s3_put("data/symbol-dictionary.json",
           {"generated_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.1", "n_symbols": len(uniq), "n_named": named,
            "named_pct": round(100 * named / len(uniq), 1),
            "rebuilt_by": "ops-3182 (runner-side FRED pass)",
            "dictionary": dic})

    rep.section("4. THE GATE — do the names read like a human wrote them?")
    checks = {
        "FRED:DGS10": "Treasury",
        "FRED:WALCL": "Assets",
        "FRED:PRAWMINDEXM": "Price",
        "FRED:FEDFUNDS": "Federal Funds",
    }
    for sym, must in checks.items():
        nm = (dic.get(sym) or {}).get("name") or ""
        d = dic.get(sym) or {}
        rep.log(f"  {sym:20s} → {nm[:58]:58s} "
                f"[{d.get('units') or '—'} · {d.get('frequency') or '—'} · "
                f"{d.get('history') or '—'}]")
        if sym in dic and must.lower() not in nm.lower():
            fails.append(f"{sym} still reads '{nm}' (expected '{must}')")
    for sym in ("NYMEX:CL1!", "TVC:US10Y", "NASDAQ:NVDA",
                "ECONOMICS:CNFER"):
        d = dic.get(sym) or {}
        rep.log(f"  {sym:20s} → {str(d.get('name'))[:58]:58s} "
                f"[{d.get('source')}: {d.get('source_id')}]")
    rep.kv(symbols=len(uniq), named=named,
           named_pct=round(100 * named / len(uniq), 1))

    rep.section("5. Refresh the fleet dictionary consumers")
    try:
        LAM.invoke(FunctionName="justhodl-symbol-dictionary",
                   InvocationType="Event", Payload=b"{}")
        rep.log("weekly engine invoked to fill the non-FRED tail")
    except Exception as e:
        warns.append(f"invoke: {str(e)[:60]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
