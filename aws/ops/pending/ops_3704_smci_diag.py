"""ops 3704 — DIAGNOSTIC: why did the read-through board miss SMCI's $60B backlog print?

Khalid: "tonight SMCI pumped hard on a 60 billion backlog contract why isn't it there?"

No guessing. This dumps exactly what the engine sees for SMCI at run time:
  1. every price field in the Polygon snapshot (day / prevDay / min / lastTrade)
     and the chg_pct the engine's gate actually computes from them
  2. the FMP news the engine reads, with the engine's own classifier and
     dollar-figure extractor applied to each headline
  3. the grouped-daily closes, so we can see whether the move exists in the
     session history at all
  4. the same for NBIS and NOK as controls

Hypothesis to test: the engine finds catalysts from a LIVE GAP (last vs prev
close). Between the end of after-hours (8pm ET) and the start of premarket
(4am ET) there is no session — day is empty and prevDay is the REGULAR close,
which excludes the after-hours pop. So an 8pm print is invisible in that window
even though the news is right there. If that is what the data shows, the fix is
to make the engine NEWS-FIRST rather than gap-first.
"""
import json
import os
import sys
import traceback
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
from ops_report import report

REGION = "us-east-1"
LAM = boto3.client("lambda", REGION)
UA = {"User-Agent": "Mozilla/5.0 (JustHodl diag)"}
NAMES = ["SMCI", "NBIS", "NOK", "DELL", "VRT"]


def envof(fn, key):
    c = LAM.get_function_configuration(FunctionName=fn)
    return (c.get("Environment", {}) or {}).get("Variables", {}).get(key, "")


def jget(url, timeout=40):
    return json.loads(urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=timeout).read())


with report("3704_smci_diag") as rep:
    rep.heading("ops 3704 — why is SMCI not on the read-through board?")
    out = {"asked_at_utc": datetime.now(timezone.utc).isoformat()}
    try:
        POLY = envof("justhodl-readthrough", "POLYGON_KEY")
        FMP = envof("justhodl-readthrough", "FMP_KEY")
        print(f"keys present: polygon={bool(POLY)} fmp={bool(FMP)}")
        print(f"now UTC {out['asked_at_utc']}  (ET = UTC-4)\n")

        # ── 1. snapshot: every field, and the gate's arithmetic ──
        snap = {}
        for n in NAMES:
            try:
                d = jget(f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/"
                         f"tickers/{n}?apiKey={POLY}")
                snap[n] = d.get("ticker") or {}
            except Exception as e:
                snap[n] = {"err": str(e)[:120]}

        print("=== POLYGON SNAPSHOT (what the catalyst gate reads) ===")
        rows = {}
        for n in NAMES:
            t = snap.get(n) or {}
            day, prev = t.get("day") or {}, t.get("prevDay") or {}
            mn, lt = t.get("min") or {}, t.get("lastTrade") or {}
            pc, dc, mc, lp = prev.get("c"), day.get("c"), mn.get("c"), lt.get("p")
            engine_last = dc or mc or pc
            engine_chg = ((engine_last / pc - 1) * 100) if (pc and engine_last) else None
            best_last = lp or dc or mc or pc
            best_chg = ((best_last / pc - 1) * 100) if (pc and best_last) else None
            rows[n] = {"prevDay_c": pc, "day_c": dc, "min_c": mc, "lastTrade_p": lp,
                       "todaysChangePerc": t.get("todaysChangePerc"),
                       "engine_last": engine_last,
                       "engine_chg_pct": round(engine_chg, 2) if engine_chg is not None else None,
                       "with_lastTrade_chg_pct": round(best_chg, 2) if best_chg is not None else None,
                       "updated_ns": t.get("updated")}
            print(f"  {n:5} prevDay.c={str(pc):>9} day.c={str(dc):>9} min.c={str(mc):>9} "
                  f"lastTrade.p={str(lp):>9}")
            print(f"        engine sees {rows[n]['engine_chg_pct']}%  |  with lastTrade "
                  f"{rows[n]['with_lastTrade_chg_pct']}%  |  polygon todaysChangePerc "
                  f"{t.get('todaysChangePerc')}")
        out["snapshot"] = rows

        # ── 2. grouped-daily: does the move exist in session history? ──
        print("\n=== GROUPED-DAILY CLOSES (last 6 sessions) ===")
        hist = {}
        for i in range(1, 8):
            ds = (date.today() - timedelta(days=i)).isoformat()
            try:
                g = jget(f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
                         f"{ds}?adjusted=true&apiKey={POLY}")
                m = {r["T"]: r["c"] for r in (g.get("results") or []) if r.get("c")}
                if m:
                    hist[ds] = {n: m.get(n) for n in NAMES}
            except Exception:
                pass
        for ds in sorted(hist):
            print(f"  {ds}  " + "  ".join(f"{n}={hist[ds].get(n)}" for n in NAMES))
        out["grouped_daily"] = hist

        # ── 3. the news the engine actually reads, with its own classifier ──
        CATALYST_KEYS = {
            "BACKLOG_ORDERS": ("backlog", "new orders", "order book", "bookings",
                               "record orders", "orders worth", "order intake", "orders totaling"),
            "MEGA_CONTRACT": ("awarded", "wins contract", "contract worth", "purchase order",
                              "agreement to supply", "supply agreement", "multi-year deal",
                              "signs deal", "selects", "partnership with"),
            "CAPACITY_EXPANSION": ("expand capacity", "new fab", "gigawatt", "capex",
                                   "capital expenditure", "build out", "data center build"),
            "GUIDANCE_RAISE": ("raises guidance", "raised guidance", "boosts outlook",
                               "lifts outlook", "raises forecast", "raises outlook"),
            "EARNINGS_BEAT": ("beats", "tops estimates", "earnings beat", "revenue beat"),
        }
        import re
        MONEY = re.compile(r"\$\s?([\d][\d,]*\.?\d*)\s*(trillion|billion|bn\b|b\b|million|mm\b|m\b)", re.I)
        MULT = {"trillion": 1e12, "billion": 1e9, "bn": 1e9, "b": 1e9,
                "million": 1e6, "mm": 1e6, "m": 1e6}

        print("\n=== FMP NEWS the engine polls (48h window, with its own classifier) ===")
        news_out = {}
        for n in NAMES[:3]:
            got = []
            for path in (f"news/stock?symbols={n}&limit=12",
                         f"news/press-releases?symbols={n}&limit=8"):
                try:
                    d = jget(f"https://financialmodelingprep.com/stable/{path}&apikey={FMP}", timeout=25)
                    if isinstance(d, list) and d:
                        got = d
                        break
                except Exception as e:
                    print(f"  [{n}] {path.split('?')[0]} -> {str(e)[:70]}")
            print(f"\n  --- {n}: {len(got)} articles ---")
            recs = []
            for a in got[:10]:
                title = (a.get("title") or "")[:150]
                pub = a.get("publishedDate") or a.get("date") or ""
                blob = f"{title} {(a.get('text') or '')[:400]}".lower()
                ctype = next((k for k, v in CATALYST_KEYS.items() if any(x in blob for x in v)),
                             "UNCLASSIFIED")
                mm = MONEY.findall(f"{title} {(a.get('text') or '')[:400]}")
                val = max([float(x.replace(",", "")) * MULT[u.lower().strip()]
                           for x, u in mm], default=None)
                recs.append({"published": pub, "type": ctype,
                             "usd": (f"${val/1e9:.1f}B" if val and val >= 1e9 else
                                     (f"${val/1e6:.0f}M" if val else None)),
                             "title": title})
                print(f"    [{pub}] {ctype:20} {str(recs[-1]['usd']):>8}  {title[:95]}")
            news_out[n] = recs
        out["news"] = news_out

        # ── 4. what the live board currently holds ──
        s3 = boto3.client("s3", REGION)
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                     Key="data/readthrough.json")["Body"].read())
        out["board_now"] = {"version": d.get("version"), "generated_at": d.get("generated_at"),
                            "events": [(e.get("ticker"), e.get("move_pct"), e.get("type"))
                                       for e in (d.get("events") or [])],
                            "params": d.get("params")}
        print(f"\n=== LIVE BOARD === v{d.get('version')} generated {d.get('generated_at')}")
        print(f"  events: {out['board_now']['events']}")
        print(f"  gate: gap_min_pct={((d.get('params') or {}).get('gap_min_pct'))}")

    except Exception:
        out["crash"] = traceback.format_exc()[-1500:]
        print("CRASH:", out["crash"][-600:])

    out["verdict"] = "CRASH" if out.get("crash") else "DIAGNOSTIC_COMPLETE"
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3704.json").write_text(json.dumps(out, indent=2, default=str))
    if out.get("crash"):
        sys.exit(1)
    sys.exit(0)
