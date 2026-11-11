"""ops 6776 - READ-ONLY audit for the "Inflows and outflows" desk (Khalid 2026-11-10 20:58 CT: "next to Macro put all the
inflows and outflows in the same heatmap format, cover as many as possible, with a historical chart with the monumental
events like Macro ... we have the engine for this somewhere that covers everything from equities to crypto to bonds").

Audit before building: (1) every flow feed the fleet already writes (ICI, ETF flows / true flows, crypto ETF + exchange
flows, stablecoins, TIC / foreign / capital inflows, global + APAC flows, capital flow, cross-asset state, hot money) -
HTTP status through the Worker, age, top-level shape, the time series inside (where they are, how many, first / last
dates, cadence); (2) what the symbol directory already serves as /series with long history for flows (money-market
assets, deposits, foreign holdings, Z.1 flows, CryptoQuant exchange flows, stablecoin supply ...). Everything is read
through the same routes the pages use; nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
FEEDS = ["ici-fund-flows", "etf-flows", "etf-true-flows", "etf-shares-history", "crypto-etf-flows", "crypto-etf-flows-history", "crypto-exchange-flows", "crypto-exchange-flows-history",
         "exchange-flows", "stablecoin-flow", "foreign-flows", "capital-inflows", "tic-flows", "global-flows", "hot-money", "apac-flows", "history/apac-flows", "capital-flow", "capital-flow-history",
         "cross-asset-flow-state", "flow-confluence", "sector-flow-state", "money-flow-state", "liquidity-flow", "etf-flows/daily", "cq-feed", "cryptoquant-onchain", "etf-fund-flows", "share-flows", "capital-flow-radar", "stealth-flow"]
QUERIES = ["fund flows", "etf flows", "money market fund assets", "mutual fund flows", "exchange inflow", "exchange netflow", "stablecoin supply", "foreign holdings treasury", "TIC net purchases",
           "net purchases of equities", "households net purchases", "bank deposits", "margin debt", "foreign portfolio investment", "capital inflows", "bitcoin etf", "ici", "creations redemptions",
           "flow of funds", "net foreign purchases", "treasury international capital", "commercial bank credit", "money fund", "exchange reserve bitcoin", "whale", "defillama tvl"]
DATE_RE = re.compile(r"^\d{4}-\d{2}(-\d{2})?")


def find_series(obj, path="", out=None, depth=0):
    """walk a feed and describe every time-series-looking structure: lists of dicts with a date key, or date-keyed dicts"""
    if out is None:
        out = []
    if depth > 6:
        return out
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        dk = next((k for k in ("date", "d", "as_of", "week", "month", "period", "t", "time", "ts", "day") if k in obj[0]), None)
        if dk:
            dates = [str(r.get(dk))[:10] for r in obj if isinstance(r, dict) and r.get(dk) is not None]
            numeric = [k for k, v in obj[0].items() if isinstance(v, (int, float)) and not isinstance(v, bool)]
            dates_sorted = sorted(d for d in dates if DATE_RE.match(d) or d.isdigit())
            out.append({"path": path, "n": len(obj), "date_key": dk, "first": dates_sorted[0] if dates_sorted else None, "last": dates_sorted[-1] if dates_sorted else None, "numeric": numeric[:12]})
            return out
        if len(obj) > 2 and isinstance(obj[0], list) and len(obj[0]) >= 2 and isinstance(obj[0][0], str) and DATE_RE.match(obj[0][0]):
            out.append({"path": path, "n": len(obj), "date_key": "[0]", "first": obj[0][0][:10], "last": obj[-1][0][:10], "numeric": ["[1]"]})
            return out
    if isinstance(obj, list) and len(obj) > 2 and isinstance(obj[0], list) and len(obj[0]) >= 2 and isinstance(obj[0][0], str) and DATE_RE.match(obj[0][0]):
        out.append({"path": path, "n": len(obj), "date_key": "[0]", "first": obj[0][0][:10], "last": obj[-1][0][:10], "numeric": ["[1]"]})
        return out
    if isinstance(obj, dict):
        keys = list(obj.keys())
        if len(keys) > 5 and sum(1 for k in keys[:20] if DATE_RE.match(str(k))) >= 15:
            ks = sorted(k for k in keys if DATE_RE.match(str(k)))
            out.append({"path": path, "n": len(ks), "date_key": "keys", "first": ks[0][:10], "last": ks[-1][:10], "numeric": [type(obj[ks[-1]]).__name__]})
            return out
        for k in keys[:400]:
            find_series(obj[k], path + "/" + str(k), out, depth + 1)
    return out


def main():
    with report("ops_6776_flows_desk_audit") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6776 - inflows / outflows desk audit %s" % now.isoformat(timespec="minutes"))
        inventory = {"feeds": {}, "directory": {}}
        r.section("1. the flow feeds the fleet writes (through the Worker /data/ route)")
        for feed in FEEDS:
            url = "%s/data/%s.json" % (PROXY, feed)
            try:
                t0 = time.time()
                resp = requests.get(url, timeout=60)
                if not resp.ok:
                    r.log("%-28s HTTP %s" % (feed, resp.status_code))
                    continue
                payload = resp.json()
                top = list(payload.keys())[:14] if isinstance(payload, dict) else ("list[%d]" % len(payload))
                gen = None
                if isinstance(payload, dict):
                    for k in ("generated_at", "as_of", "updated", "updated_at", "timestamp", "generated", "run_at"):
                        if k in payload:
                            gen = payload[k]
                            break
                series = find_series(payload)
                inventory["feeds"][feed] = {"bytes": len(resp.content), "generated": gen, "top": top, "series": series[:40]}
                r.log("%-28s %7d KB | generated %s | top %s | %d series-like structures | %.1fs" % (feed, len(resp.content) // 1024, str(gen)[:25], str(top)[:120], len(series), time.time() - t0))
                for s in series[:12]:
                    r.log("    %-60s n=%-5s %s → %s  numeric %s" % (s["path"][:60], s["n"], s["first"], s["last"], str(s["numeric"])[:70]))
                if len(series) > 12:
                    r.log("    … %d more" % (len(series) - 12))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s %s" % (feed, type(e).__name__, str(e)[:160]))
        r.section("2. what the directory already serves as /series for flows")
        seen = {}
        for q in QUERIES:
            try:
                resp = requests.get("%s/symsearch?q=%s&limit=30" % (PROXY, requests.utils.quote(q)), timeout=60)
                payload = resp.json() if resp.ok else {}
                rows = ((payload.get("series_hits") or {}).get("rows") or []) + [row for row in (payload.get("rows") or []) if row.get("kind") in ("series", "dataset")]
                facets = ", ".join("%s %s" % (f.get("provider"), f.get("n")) for f in (payload.get("facets") or [])[:8])
                r.log("'%s': %s indexed | %s | %s" % (q, payload.get("total"), facets[:120], "; ".join("%s (%s)" % (row.get("id"), str(row.get("title") or row.get("name") or "")[:45]) for row in rows[:6])))
                for row in rows[:12]:
                    if row.get("kind") == "series" and row.get("id") not in seen:
                        seen[row["id"]] = {"name": row.get("title") or row.get("name"), "provider": row.get("provider_name") or row.get("provider"), "q": q, "first": row.get("first"), "last": row.get("last"), "freq": row.get("freq"), "n": row.get("n")}
            except Exception as e:  # noqa: BLE001
                r.warn("symsearch '%s': %s" % (q, type(e).__name__))
        # confirm history for a shortlist by reading /series
        shortlist = ["fred:WRMFNS", "fred:WIMFNS", "fred:MMMFFAQ027S", "fred:DPSACBW027SBOG", "fred:TOTBKCR", "fred:FDHBFIN", "fred:BOGZ1FA153064105Q", "fred:BOGZ1FA153064005Q", "fred:BOGMBASE", "fred:WTREGEN", "fred:RRPONTSYD", "fred:WALCL",
                     "fred:FRGSHPUSM649NCIS", "fred:BOGZ1FL153064105Q", "fred:TOTALSL", "fred:DGS10"]
        shortlist += [sid for sid in seen if sid not in shortlist][:40]
        for sid in shortlist:
            try:
                resp = requests.get("%s/series?id=%s" % (PROXY, requests.utils.quote(sid)), timeout=60)
                payload = resp.json() if resp.ok else {}
                obs = payload.get("obs") or payload.get("bars") or []
                first = str(obs[0][0] if isinstance(obs[0], list) else (obs[0].get("time") or obs[0].get("date")))[:10] if obs else None
                last = str(obs[-1][0] if isinstance(obs[-1], list) else (obs[-1].get("time") or obs[-1].get("date")))[:10] if obs else None
                inventory["directory"][sid] = {"name": payload.get("name") or payload.get("title") or (seen.get(sid) or {}).get("name"), "unit": payload.get("unit") or payload.get("units"), "n": len(obs), "first": first, "last": last, "http": resp.status_code}
                r.log("%-34s HTTP %s | %5d obs | %s → %s | %s | %s" % (sid, resp.status_code, len(obs), first, last, str(payload.get("unit") or payload.get("units") or "")[:18], str(payload.get("name") or payload.get("title") or (seen.get(sid) or {}).get("name") or "")[:60]))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (sid, type(e).__name__))
        blob = base64.b64encode(gzip.compress(json.dumps(inventory, separators=(",", ":"), default=str).encode())).decode()
        r.section("inventory (gzip+base64 JSON)")
        r.log("<!-- FLOWS_AUDIT_GZB64 -->\n%s\n<!-- /FLOWS_AUDIT_GZB64 -->" % blob)
        r.ok("ops 6776 complete (read-only): %d feeds read, %d directory series checked" % (len(inventory["feeds"]), len(inventory["directory"])))


if __name__ == "__main__":
    main()
    sys.exit(0)
