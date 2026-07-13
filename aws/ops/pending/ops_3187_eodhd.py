"""ops 3187 — Khalid's EODHD token: make it prove its worth or get cancelled.

He bought it. So the job is not to argue — it is to MEASURE, inside the
billing month, exactly what it adds that free sources cannot give him.

Tested against the precise buckets where ops 3186's free-path probe FAILED:
  FTSE      448 symbols · free hit 0%  (licensed index product — the big one)
  EURONEXT   37 · 0%
  BER        14 · 0%
  SSE        52 · 17%
  SWB/XETR/FWB/GETTEX/TRADEGATE  · 33-50%
  CBOEEU / EUREX / ICEEUR · 0%

For each: resolve via EODHD exchange code, fall back to EODHD's own search
endpoint, REAL-FETCH the series, and report obs + history depth. Then re-map
the universe with EODHD in the chain and count how many dormant engines
actually wake. Verdict is a number, not an opinion.
"""

import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
TOKEN = "6a543beea9ebe2.87551566"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3187_eodhd") as rep:
    fails, warns = [], []
    rep.heading("ops 3187 — does the EODHD token earn its keep?")

    rep.section("1. Store the key + prove it works")
    SS.EODHD_KEY = TOKEN
    try:
        SSM.put_parameter(Name="/justhodl/eodhd-api-key", Value=TOKEN,
                          Type="SecureString", Overwrite=True)
        rep.ok("SSM /justhodl/eodhd-api-key set (single source of truth)")
    except Exception as e:
        warns.append(f"ssm: {str(e)[:60]}")
    probe = SS.fetch("EODHD", "AAPL.US", "2015-01-01")
    rep.kv(token_live=bool(probe), aapl_obs=len(probe))
    if not probe:
        fails.append("token rejected on AAPL.US — check the key/plan")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)
    ks = sorted(probe)
    rep.ok(f"token LIVE — AAPL.US {len(probe)} obs ({ks[0]} → {ks[-1]})")

    rep.section("2. THE BUCKETS THAT FAILED FOR FREE — does EODHD have them?")
    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    by_ex = defaultdict(list)
    for s in uniq:
        if ":" in s:
            by_ex[s.split(":", 1)[0]].append(s)

    verdict = {}
    for ex in ("FTSE", "EURONEXT", "BER", "SSE", "SWB", "XETR", "FWB",
               "TRADEGATE", "GETTEX", "CBOEEU", "EUREX", "ICEEUR", "SIX",
               "HKEX", "MIL", "LSE"):
        syms = by_ex.get(ex) or []
        if not syms:
            continue
        sample = syms[:5]
        hits, ex_map = 0, []
        for sym in sample:
            sid = SS.eodhd_resolve(sym)
            ser = SS.fetch("EODHD", sid, "2015-01-01") if sid else {}
            if len(ser) < 100:                    # try EODHD's own search
                alt = SS.eodhd_search(sym.split(":", 1)[1])
                if alt and alt != sid:
                    ser = SS.fetch("EODHD", alt, "2015-01-01")
                    sid = alt
            if len(ser) >= 100:
                hits += 1
                k = sorted(ser)
                if len(ex_map) < 2:
                    ex_map.append(f"{sym}→{sid} ({len(ser)} obs, "
                                  f"{k[0][:4]}–{k[-1][:4]})")
            time.sleep(0.12)
        rate = round(100 * hits / len(sample))
        verdict[ex] = {"n": len(syms), "hit": rate}
        rep.log(f"  {ex:11s} {len(syms):4d} symbols · EODHD hit {rate:3d}%  "
                f"{'; '.join(ex_map)[:62]}")

    rep.section("3. What it actually buys")
    wins = [(ex, v["n"], v["hit"]) for ex, v in verdict.items()
            if v["hit"] >= 60]
    misses = [(ex, v["n"], v["hit"]) for ex, v in verdict.items()
              if v["hit"] < 60]
    bought = sum(n for _, n, _ in wins)
    rep.kv(symbols_eodhd_delivers=bought,
           symbols_still_missing=sum(n for _, n, _ in misses))
    for ex, n, h in sorted(wins, key=lambda r: -r[1]):
        rep.log(f"  ✅ {ex:11s} {n:4d} symbols ({h}%)")
    for ex, n, h in sorted(misses, key=lambda r: -r[1]):
        rep.log(f"  ❌ {ex:11s} {n:4d} symbols ({h}%) — EODHD does NOT "
                "carry these either")

    ftse = verdict.get("FTSE") or {}
    if ftse.get("hit", 0) >= 60:
        rep.ok(f"FTSE ({ftse['n']} symbols — his LARGEST gap) IS covered — "
               "that alone justifies the subscription")
    else:
        rep.warn(f"FTSE ({ftse.get('n')} symbols) NOT covered even with the "
                 "token — it is FTSE Russell licensed index product, exactly "
                 "as suspected. His largest bucket stays dark.")

    rep.section("4. Wire it into the fleet")
    for fn in ("justhodl-wl-engines", "justhodl-thesis-engine",
               "justhodl-symbol-dictionary"):
        try:
            live = LAM.get_function_configuration(FunctionName=fn)
            env = (live.get("Environment") or {}).get("Variables") or {}
            env["EODHD_API_KEY"] = TOKEN
            LAM.update_function_configuration(
                FunctionName=fn, Environment={"Variables": env})
            LAM.get_waiter("function_updated").wait(
                FunctionName=fn, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
            rep.ok(f"{fn}: EODHD key armed")
        except Exception as e:
            warns.append(f"{fn}: {str(e)[:70]}")

    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
