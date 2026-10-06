#!/usr/bin/env python3
"""Build compact per-symbol insight files for the chart watchlist and symbol page (2026-10-06).

Reads only published JustHodl research artifacts (no new providers, no inference):
  * data/etf-holdings-research.json  -> ETF Global constituent snapshots for ~300 ETFs (current vs ~30 days prior)
  * data/13f-flows-by-ticker.json    -> SEC 13F complete fund artifacts for the tracked large managers
Writes (to OUT, default ./watchlist-insights):
  etf/<ETF>.json     composition (all reported constituents), reported adds/removals/size changes vs the prior snapshot
  stock/<A-Z|_>.json per-constituent: ETFs reporting it, ETFs that added/removed it between snapshots, 13F manager changes
  index.json         build metadata and coverage counts
Values are copied from the source rows. Reported position changes are not trades; that wording travels with the data.
"""
import concurrent.futures as cf
import json
import os
import re
import sys
import time
import urllib.request
from decimal import Decimal, InvalidOperation

BASE = os.environ.get("JH_BASE", "https://justhodl.ai/")
OUT = sys.argv[1] if len(sys.argv) > 1 else "watchlist-insights"
UA = {"User-Agent": "Mozilla/5.0 (JustHodl watchlist-insights builder)", "Origin": "https://justhodl.ai"}


def get(key, tries=4):
    url = key if key.startswith("http") else BASE + key.lstrip("/")
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=120) as r:
                return json.load(r)
        except Exception as e:  # noqa: BLE001
            if i == tries - 1:
                print("WARN fetch failed", key, e, file=sys.stderr)
                return None
            time.sleep(1.5 * (i + 1))


def dec(s):
    if s is None:
        return None
    try:
        v = Decimal(str(s))
    except (InvalidOperation, ValueError):
        return None
    return float(v)


def parts(manifest):
    out = []
    for p in (manifest or {}).get("parts") or []:
        d = get(p["key"])
        if d and isinstance(d.get("rows"), list):
            out.extend(d["rows"])
    return out


def shard(tk):
    """Two-character shard so a browser reads ~10-60 KB for one symbol."""
    k = re.sub(r"[^A-Z0-9]", "_", (tk[:2].upper() + "__")[:2])
    return k


def build_etf(tk, f):
    cur_snap = get(f["current"]["snapshot"]["key"]) if (f.get("current") or {}).get("snapshot") else None
    if not cur_snap:
        return None
    cur_rows = parts(cur_snap)
    prior_rows, cmp_rows = [], []
    if (f.get("prior") or {}).get("snapshot"):
        ps = get(f["prior"]["snapshot"]["key"])
        prior_rows = parts(ps) if ps else []
    if f.get("comparison"):
        cm = get(f["comparison"]["key"])
        cmp_rows = parts(cm) if cm else []
    return tk, f, cur_rows, prior_rows, cmp_rows


def row_brief(r):
    return {
        "t": (r.get("constituent_ticker") or "").strip().upper() or None,
        "n": (r.get("constituent_name") or "").strip(),
        "w": dec(r.get("weight_raw_decimal")),
        "sh": dec(r.get("shares_held_raw_decimal")),
        "mv": dec(r.get("market_value_raw_decimal")),
        "cusip": r.get("us_code"),
        "isin": r.get("isin"),
        "ac": r.get("asset_class"),
        "st": r.get("security_type"),
        "eff": r.get("effective_date"),
    }


def main():
    t0 = time.time()
    ehr = get("data/etf-holdings-research.json")
    if not ehr or "funds" not in ehr:
        sys.exit("etf-holdings-research.json unavailable")
    funds = ehr["funds"]
    stocks = {}  # ticker -> record
    cusip_to_tk = {}

    def S(tk, name=None, cusip=None):
        s = stocks.get(tk)
        if not s:
            s = stocks[tk] = {"name": name or "", "cusip": cusip, "etf": {"held": [], "added": [], "removed": [], "up": 0, "down": 0}}
        if name and not s["name"]:
            s["name"] = name
        if cusip and not s.get("cusip"):
            s["cusip"] = cusip
        return s

    os.makedirs(os.path.join(OUT, "etf"), exist_ok=True)
    os.makedirs(os.path.join(OUT, "stock"), exist_ok=True)
    jobs = [(tk, f) for tk, f in funds.items() if (f.get("current") or {}).get("acquisition_status") == "complete_returned_snapshot"]
    n_etf = 0
    with cf.ThreadPoolExecutor(16) as ex:
        for res in ex.map(lambda a: build_etf(*a), jobs):
            if not res:
                continue
            tk, f, cur_rows, prior_rows, cmp_rows = res
            byid_c = {r["row_id"]: r for r in cur_rows if r.get("row_id")}
            byid_p = {r["row_id"]: r for r in prior_rows if r.get("row_id")}
            cur = [row_brief(r) for r in cur_rows]
            cur.sort(key=lambda x: -(x["w"] or 0))
            # provider weights arrive either as fractions (SPY sums to ~1.0, 2x funds to ~2.0) or as percents (~100)
            wsum = sum(abs(r["w"] or 0) for r in cur)
            scale = 1.0 if wsum > 20 else 100.0
            for coll in (cur,):
                for r in coll:
                    r["pct"] = None if r["w"] is None else round(r["w"] * scale, 6)
            eff = sorted((f["current"].get("effective_dates") or {}).keys())
            peff = sorted(((f.get("prior") or {}).get("effective_dates") or {}).keys())
            added, removed, up, down = [], [], [], []
            for c in cmp_rows:
                st = c.get("status")
                if st == "observed_only_in_current":
                    r = next((byid_c[i] for i in c.get("current_rows") or [] if i in byid_c), None)
                    if r:
                        added.append(row_brief(r))
                elif st == "observed_only_in_prior":
                    r = next((byid_p[i] for i in c.get("prior_rows") or [] if i in byid_p), None)
                    if r:
                        removed.append(row_brief(r))
                elif st == "observed_in_both":
                    ch = dec(c.get("shares_held_change_raw_decimal"))
                    r = next((byid_c[i] for i in c.get("current_rows") or [] if i in byid_c), None)
                    if r is not None and ch:
                        b = row_brief(r); b["chg"] = ch
                        (up if ch > 0 else down).append(b)
            ac = {}
            for r in cur:
                k = r["ac"] or "Unreported"
                ac[k] = round(ac.get(k, 0) + (r["pct"] or 0), 6)
            doc = {
                "contract": "jh-watchlist-etf-insight.v1", "ticker": tk,
                "source": "ETF Global constituents via data/etf-holdings-research.json",
                "effective": eff, "prior_effective": peff, "constituents": len(cur),
                "weight_unit": "percent of reported weights (provider raw weights x%d; raw sum %.4f)" % (scale, wsum),
                "category": (f.get("configured_tag_unverified") or {}),
                "holdings": [[r["t"], r["n"], r["pct"], r["sh"], r["mv"], r["cusip"], r["ac"]] for r in cur],
                "asset_class_weight": ac,
                "added": [[r["t"], r["n"], None if r["w"] is None else round(r["w"] * scale, 6)] for r in added],
                "removed": [[r["t"], r["n"], None if r["w"] is None else round(r["w"] * scale, 6)] for r in removed],
                "increased": len(up), "decreased": len(down),
                "top_increases": [[r["t"], r["n"], r["chg"]] for r in sorted(up, key=lambda x: -abs(x["chg"]))[:15]],
                "top_decreases": [[r["t"], r["n"], r["chg"]] for r in sorted(down, key=lambda x: -abs(x["chg"]))[:15]],
                "note": "Reported constituent changes between dated snapshots; not trades, flows or index events.",
            }
            with open(os.path.join(OUT, "etf", tk + ".json"), "w") as fh:
                json.dump(doc, fh, separators=(",", ":"))
            n_etf += 1
            when = (eff[-1] if eff else None)
            for r in cur:
                if not r["t"]:
                    continue
                if r["cusip"]:
                    cusip_to_tk.setdefault(r["cusip"], r["t"])
                S(r["t"], r["n"], r["cusip"])["etf"]["held"].append([tk, r["pct"], r["sh"], when])
            for r in added:
                if r["t"]:
                    S(r["t"], r["n"], r["cusip"])["etf"]["added"].append([tk, None if r["w"] is None else round(r["w"] * scale, 6), when])
            for r in removed:
                if r["t"]:
                    S(r["t"], r["n"], r["cusip"])["etf"]["removed"].append([tk, peff[-1] if peff else None])
            for r in up:
                if r["t"]:
                    S(r["t"])["etf"]["up"] += 1
            for r in down:
                if r["t"]:
                    S(r["t"])["etf"]["down"] += 1
    print("ETFs built", n_etf, "constituent tickers", len(stocks), round(time.time() - t0), "s")

    # ---- 13F large managers
    f13 = get("data/13f-flows-by-ticker.json") or {}
    managers = []
    for fk, meta in (f13.get("funds") or {}).items():
        if not meta.get("current_cohort_eligible") or not meta.get("detail"):
            continue
        managers.append((fk, meta))
    period = f13.get("required_period_for_current_cohort")

    def load_fund(a):
        fk, meta = a
        return fk, meta, get(meta["detail"]["key"])

    unmapped = 0
    mgr_done = []
    with cf.ThreadPoolExecutor(6) as ex:
        for fk, meta, d in ex.map(load_fund, managers):
            if not d:
                continue
            cp = d.get("current_holdings_period")
            pos = ((d.get("periods") or {}).get(cp) or {}).get("positions") or {}
            prior_pos = ((d.get("periods") or {}).get(d.get("prior_holdings_period")) or {}).get("positions") or {}
            filed = (meta.get("latest_submission") or {}).get("filed_at")
            mgr_done.append({"fund": fk, "name": meta.get("official_name") or meta.get("name"), "period": cp, "prior": d.get("prior_holdings_period"), "filed": filed})
            for row in (d.get("comparison") or {}).get("rows") or []:
                idn = row.get("identity") or {}
                if idn.get("put_call") or idn.get("quantity_type") != "SH":
                    continue
                tk = cusip_to_tk.get(idn.get("cusip"))
                if not tk:
                    unmapped += 1
                    continue
                p = pos.get(row.get("position_id")) or {}
                pp = prior_pos.get(row.get("position_id")) or {}
                s = S(tk)
                s.setdefault("f13", {"period": cp, "rows": []})
                s["f13"]["rows"].append([fk, row.get("status"), dec(p.get("reported_quantity")), dec(row.get("reported_quantity_change")), dec(p.get("reported_value_usd")), dec(pp.get("reported_quantity"))])
    print("13F managers", len(mgr_done), "unmapped equity rows", unmapped)

    by_shard = {}
    for tk, s in stocks.items():
        s["etf"]["held"].sort(key=lambda x: -(x[1] or 0))
        s["etf"]["n_held"] = len(s["etf"]["held"])
        s["etf"]["held"] = s["etf"]["held"][:60]
        by_shard.setdefault(shard(tk), {})[tk] = s
    for k, v in by_shard.items():
        with open(os.path.join(OUT, "stock", k + ".json"), "w") as fh:
            json.dump({"contract": "jh-watchlist-stock-insight.v1", "shard": k, "managers": mgr_done, "rows": v}, fh, separators=(",", ":"))
    idx = {
        "contract": "jh-watchlist-insights-index.v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "etf_source_generated_at": ehr.get("generated_at"), "f13_source_generated_at": f13.get("generated_at"),
        "etfs": n_etf, "constituent_tickers": len(stocks), "f13_period": period, "f13_managers": mgr_done,
        "limitations": [
            "ETF constituent changes compare two dated provider snapshots (about 30 days apart); they are reported position changes, not trades or flows.",
            "13F rows are quarter-end long equity disclosures of the tracked large managers only; options and shorts are excluded.",
            "13F CUSIPs map to tickers only through exact ETF Global constituent CUSIPs; unmapped CUSIPs are omitted, not zero.",
        ],
    }
    with open(os.path.join(OUT, "index.json"), "w") as fh:
        json.dump(idx, fh, indent=1)
    print("done", round(time.time() - t0), "s")


if __name__ == "__main__":
    main()
