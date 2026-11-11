"""ops 6777 - READ-ONLY follow-up to the flows audit (ops 6776): the exact shapes of the fleet's flow feeds that carry
time series (so the Inflows & outflows desk can read them), the names of the TIC / Z.1 directory series, and the ICI
dataset's series ids. Prints compact samples; embeds them gz+base64. Nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import sys

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"


def get(url, timeout=60):
    resp = requests.get(url, timeout=timeout)
    return resp.status_code, (resp.json() if resp.ok and resp.headers.get("content-type", "").find("json") >= 0 or resp.ok else None)


def sample(obj, n=2):
    """a compact sample: first/last rows of lists, keys of dicts"""
    if isinstance(obj, list):
        return {"list": len(obj), "first": sample(obj[0], n) if obj else None, "last": sample(obj[-1], n) if len(obj) > 1 else None}
    if isinstance(obj, dict):
        out = {}
        for k in list(obj.keys())[:40]:
            v = obj[k]
            out[k] = sample(v, n) if isinstance(v, (list, dict)) else (v if isinstance(v, (int, float, bool)) or v is None else str(v)[:60])
        if len(obj) > 40:
            out["…"] = "%d keys" % len(obj)
        return out
    return obj


def main():
    with report("ops_6777_flows_feed_shapes") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6777 - flow feed shapes %s" % now.isoformat(timespec="minutes"))
        shapes = {}
        probes = {
            "ici-fund-flows": ["latest_week", "history", "units", "count", "sources"],
            "crypto-etf-flows-history": ["series"],
            "crypto-etf-flows": None,
            "crypto-exchange-flows-history": ["series", "cq_flows"],
            "crypto-exchange-flows": None,
            "exchange-flows": ["btc", "eth"],
            "stablecoin-flow": None,
            "etf-flows": ["category_rotation", "n_etfs_analyzed", "measurement_type", "top_inflows", "top_outflows", "totals"],
            "etf-flows/daily": ["by_category", "composite", "canonical", "events"],
            "apac-flows": ["taiwan", "korea", "japan", "thesis"],
            "history/apac-flows": ["days"],
            "foreign-flows": ["absorption", "country_lt_treasury", "top_holders", "monthly_transactions", "as_of"],
            "tic-flows": ["composite_tic_stress", "foreign_transactions", "history", "as_of"],
            "capital-inflows": ["by_asset_class", "data_asof", "history", "series"],
            "hot-money": ["countries", "as_of", "decision"],
            "global-flows": ["countries", "composites", "as_of"],
            "cq-feed": ["metrics", "n_metrics"],
            "cryptoquant-onchain": ["label", "grading", "n_metrics", "metrics", "series"],
            "liquidity-flow": ["current", "history_180d"],
            "cross-asset-flow-state": ["asset_class_rotation", "category_total"],
        }
        r.section("1. feed shapes")
        for feed, keys in probes.items():
            try:
                code, payload = get("%s/data/%s.json" % (PROXY, feed))
                if not isinstance(payload, (dict, list)):
                    r.log("%s: HTTP %s" % (feed, code))
                    continue
                if keys is None or not isinstance(payload, dict):
                    s = sample(payload)
                else:
                    s = {k: sample(payload.get(k)) for k in keys if k in payload}
                    s["_all_keys"] = list(payload.keys())[:60]
                shapes[feed] = s
                r.log("%s:\n%s" % (feed, json.dumps(s, default=str)[:2400]))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s %s" % (feed, type(e).__name__, str(e)[:160]))
        r.section("2. directory: ICI dataset series, TIC holder series names, Z.1 fund-flow ids")
        for ds in ("ici-flows",):
            try:
                code, payload = get("%s/browse?ds=%s&q=&limit=60&offset=0" % (PROXY, ds))
                rows = (payload or {}).get("rows") or []
                shapes["browse:" + ds] = rows[:60]
                r.log("browse %s: HTTP %s, %d rows: %s" % (ds, code, len(rows), "; ".join("%s (%s %s→%s)" % (row.get("id"), str(row.get("name") or row.get("title") or "")[:40], row.get("first"), row.get("last")) for row in rows[:20])))
            except Exception as e:  # noqa: BLE001
                r.warn("browse %s: %s" % (ds, type(e).__name__))
        names = {}
        for sid in ["fred:FORTREASPOS41408", "fred:FORTREASPOS13005", "fred:FORTREASPOS69995", "fred:FORTREASPOS42609", "fred:FORLTTREASPOS41408", "fred:FORLTTREASPOS42609", "fred:BOGZ1FA153064005Q", "fred:MMMFTAQ027S", "fred:MMMFFAQ027S"]:
            try:
                code, payload = get("%s/series?id=%s" % (PROXY, requests.utils.quote(sid)))
                obs = (payload or {}).get("obs") or []
                names[sid] = {"name": (payload or {}).get("name") or (payload or {}).get("title"), "unit": (payload or {}).get("unit") or (payload or {}).get("units"), "n": len(obs), "first": obs[0][0] if obs else None, "last": obs[-1][0] if obs else None, "last_value": obs[-1][1] if obs else None}
                r.log("%-28s %s | %s | %s → %s | last %s" % (sid, names[sid]["name"], names[sid]["unit"], names[sid]["first"], names[sid]["last"], names[sid]["last_value"]))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (sid, type(e).__name__))
        shapes["names"] = names
        found = {}
        for q in ["Mutual Funds; Corporate Equities; Asset, Transactions", "Exchange-Traded Funds; Total Financial Assets; Transactions", "Rest of the World; Treasury Securities; Asset, Transactions", "Rest of the World; Corporate Equities; Asset, Transactions",
                  "Households; Mutual Fund Shares; Asset, Transactions", "Money Market Funds; Total Financial Assets; Transactions", "Pension Funds; Total Financial Assets; Transactions", "Exchange-Traded Funds; Corporate Equities; Asset, Transactions",
                  "Households; Corporate Equities; Asset, Transactions", "Rest of the World; Total Financial Assets; Transactions", "Nonfinancial Corporate Business; Corporate Equities; Liability, Transactions", "Foreign Purchases of U.S. Long-Term Securities",
                  "Net Foreign Purchases of U.S. Treasury Bonds and Notes", "FORLTTREASNET", "Margin accounts debit balances", "Retail Money Market Funds", "Institutional Money Market Funds", "Deposits All Commercial Banks", "Bank Credit All Commercial Banks"]:
            try:
                code, payload = get("%s/symsearch?q=%s&provider=fred&limit=8" % (PROXY, requests.utils.quote(q)))
                rows = ((payload or {}).get("series_hits") or {}).get("rows") or [row for row in ((payload or {}).get("rows") or []) if row.get("kind") == "series"]
                found[q] = [(row.get("id"), str(row.get("title") or row.get("name") or "")[:80], row.get("first"), row.get("last"), row.get("freq")) for row in rows[:5]]
                r.log("'%s': %s" % (q, "; ".join("%s (%s %s→%s %s)" % t for t in found[q][:4])))
            except Exception as e:  # noqa: BLE001
                r.warn("'%s': %s" % (q, type(e).__name__))
        shapes["z1_search"] = found
        blob = base64.b64encode(gzip.compress(json.dumps(shapes, separators=(",", ":"), default=str).encode())).decode()
        r.section("shapes (gzip+base64 JSON)")
        r.log("<!-- FLOWS_SHAPES_GZB64 -->\n%s\n<!-- /FLOWS_SHAPES_GZB64 -->" % blob)
        r.ok("ops 6777 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
