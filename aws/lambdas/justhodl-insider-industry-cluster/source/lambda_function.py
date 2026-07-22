"""justhodl-insider-industry-cluster v1.0 — PEER-GROUP INSIDER CONVICTION (#16).

Canary list item #16: "not one CEO, but 4+ across peers." A single executive
buying his own stock is idiosyncratic — he may be exercising options, hitting an
ownership guideline, or simply optimistic about one company. Four executives at
four DIFFERENT companies in the same industry buying inside one window is a
different object entirely: it says the people with the best view of an
industry's order book are all reaching the same conclusion at once.

AUDIT (ops 3742, extend-don't-duplicate):
  · insider-cluster-scanner clusters WITHIN a ticker and — critically — DROPS
    any ticker with a single insider (line 535). Those discarded rows ARE the
    peer-group signal, so ops 3743 added an additive `all_ticker_buys` sidecar
    (schema 2.1) carrying every ticker with qualifying buys, pre-filter.
  · industry-boom already folds `insider_buys_30d` into a composite at weight
    10 — a COUNT inside a score, not a cluster detector with breadth, role
    conviction, recency and concentration structure.
  · insider-radar carries sector but not industry, and only 60 rows.
  No engine answers "which industries are seeing BROAD insider accumulation".

WHY BREADTH IS MEASURED ACROSS COMPANIES, NOT TRANSACTIONS
  Counting transactions rewards one insider filing five times. Counting dollars
  rewards a single large buyer. The canary is DISTINCT COMPANIES — so breadth
  is n_companies, and everything else is a modifier on top of it.

GUARDS AGAINST FALSE POSITIVES (the reason this needs care)
  · Biotech and regional banks ALWAYS show scattered insider buying — hundreds
    of small caps, routine option exercises. A raw count crowns them every day.
    Fix: participation RATE (buying companies ÷ listed companies in industry)
    alongside the raw count, and a base-rate ledger so an industry is only
    flagged when it is elevated VS ITS OWN history.
  · Tiny industries: a 3-name industry with 2 buyers is 67% participation and
    means nothing. Floor on listed count.
  · Role conviction: CEO/CFO purchases outrank director purchases; role_tier
    comes through from the scanner.
  · Dollar concentration: if one company is 90% of the dollars, breadth is
    cosmetic — HHI is published so the reader can see it.

OUTPUT data/insider-industry-cluster.json — per-industry rows, ranked, with a
signal ladder and an honest coverage block. History ledger self-builds; the
base-rate z activates at n>=8 observations.
"""
import json
from collections import defaultdict
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/insider-industry-cluster.json"
HIST_KEY = "data/insider-industry-cluster-history.json"
S3 = boto3.client("s3", region_name="us-east-1")

MIN_COMPANIES = 3          # below this it is not a cluster
MIN_LISTED = 8             # tiny industries produce meaningless rates
STRONG_COMPANIES = 4       # Khalid's "4+ across peers"
HIST_MIN = 8               # observations before z activates


def _load(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print("[iic] load fail", key, str(e)[:80])
        return None


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    degraded = []

    # ── ticker -> industry / sector / mcap from the universe ─────────────
    uni = _load("data/universe.json") or {}
    ind_of, sec_of, mcap_of = {}, {}, {}
    listed = defaultdict(int)
    for s0 in uni.get("stocks") or []:
        t = (s0.get("symbol") or "").upper()
        ind = (s0.get("industry") or "").strip()
        if not t or not ind or ind.lower() == "unknown":
            continue
        ind_of[t] = ind
        sec_of[t] = (s0.get("sector") or "").strip()
        mcap_of[t] = _f(s0.get("market_cap"))
        listed[ind] += 1
    if not ind_of:
        degraded.append("universe missing — no industry map")

    # ── insider buys: prefer the breadth sidecar, fall back to clusters ──
    clus = _load("data/insider-clusters.json") or {}
    rows = clus.get("all_ticker_buys") or []
    source = "all_ticker_buys"
    if not rows:
        rows = clus.get("clusters") or []
        source = "clusters(filtered)"
        degraded.append("breadth sidecar absent — using filtered clusters, "
                        "single-insider companies are invisible")
    lookback = clus.get("lookback_days")

    # ── aggregate to industry ────────────────────────────────────────────
    agg = defaultdict(lambda: {"companies": [], "value": 0.0, "txns": 0,
                               "insiders": 0, "max_tier": 0,
                               "ceo_cfo_names": [], "per_co": {}})
    unmapped = 0
    for r in rows:
        t = (r.get("ticker") or r.get("symbol") or "").upper()
        if not t:
            continue
        ind = ind_of.get(t)
        if not ind:
            unmapped += 1
            continue
        a = agg[ind]
        val = _f(r.get("total_value"))
        a["companies"].append(t)
        a["value"] += val
        a["per_co"][t] = a["per_co"].get(t, 0.0) + val
        a["txns"] += int(r.get("n_transactions") or 0)
        a["insiders"] += int(r.get("n_insiders") or 0)
        tier = int(r.get("max_role_tier") or 0)
        # scanner rows carry has_ceo/has_cfo booleans; sidecar carries tier
        if r.get("has_ceo") or r.get("has_cfo"):
            tier = max(tier, 100)
        a["max_tier"] = max(a["max_tier"], tier)
        if r.get("has_ceo") or r.get("has_cfo") or tier >= 100:
            a["ceo_cfo_names"].append(t)

    hist = _load(HIST_KEY) or {"obs": {}}
    today = started.strftime("%Y-%m-%d")

    out_rows = []
    for ind, a in agg.items():
        comps = sorted(set(a["companies"]))
        n_co = len(comps)
        n_listed = listed.get(ind, 0)
        if n_co < MIN_COMPANIES:
            continue
        rate = (n_co / n_listed * 100) if n_listed else None
        # dollar concentration across the buying companies
        tot = sum(a["per_co"].values()) or 0.0
        hhi = (sum((v / tot) ** 2 for v in a["per_co"].values())
               if tot > 0 else None)
        top_co, top_val = (max(a["per_co"].items(), key=lambda kv: kv[1])
                           if a["per_co"] else ("", 0.0))

        # self-building base rate: is this industry unusual FOR ITSELF?
        rec = hist["obs"].setdefault(ind, {})
        rec[today] = n_co
        vals = [v for v in rec.values() if isinstance(v, (int, float))]
        z = None
        if len(vals) >= HIST_MIN:
            mu = sum(vals) / len(vals)
            sd = (sum((v - mu) ** 2 for v in vals) / len(vals)) ** 0.5
            if sd > 0:
                z = round((n_co - mu) / sd, 2)

        out_rows.append({
            "industry": ind,
            "sector": next((sec_of[c] for c in comps if sec_of.get(c)), ""),
            "n_companies": n_co,
            "n_listed": n_listed,
            "participation_pct": round(rate, 2) if rate is not None else None,
            "n_insiders": a["insiders"],
            "n_transactions": a["txns"],
            "total_value_usd": round(a["value"], 2),
            "companies": comps[:24],
            "ceo_cfo_companies": sorted(set(a["ceo_cfo_names"]))[:12],
            "has_exec_conviction": bool(a["ceo_cfo_names"]),
            "dollar_hhi": round(hhi, 3) if hhi is not None else None,
            "top_company": top_co,
            "top_company_share_pct": (round(top_val / tot * 100, 1)
                                      if tot > 0 else None),
            "z_vs_own_history": z,
            "hist_n": len(vals),
            "thin_universe": bool(n_listed and n_listed < MIN_LISTED),
        })

    # ── signal ladder ────────────────────────────────────────────────────
    for r in out_rows:
        n_co = r["n_companies"]
        conc = r["dollar_hhi"]
        tier = None
        if r["thin_universe"]:
            tier = "THIN_UNIVERSE"           # published, never promoted
        elif n_co >= STRONG_COMPANIES and r["has_exec_conviction"] and (
                r["z_vs_own_history"] is None or r["z_vs_own_history"] >= 1.0):
            tier = "PEER_CLUSTER_CONFIRMED"
        elif n_co >= STRONG_COMPANIES and r["has_exec_conviction"]:
            tier = "PEER_CLUSTER_EXEC"
        elif n_co >= STRONG_COMPANIES:
            tier = "PEER_CLUSTER_BROAD"
        else:
            tier = "EMERGING"
        if conc is not None and conc >= 0.75 and tier.startswith("PEER"):
            tier += "_CONCENTRATED"          # breadth is cosmetic; say so
        r["tier"] = tier

    order = {"PEER_CLUSTER_CONFIRMED": 0, "PEER_CLUSTER_EXEC": 1,
             "PEER_CLUSTER_BROAD": 2, "EMERGING": 3, "THIN_UNIVERSE": 4}
    out_rows.sort(key=lambda r: (order.get(r["tier"].replace("_CONCENTRATED", ""), 9),
                                 -r["n_companies"], -(r["total_value_usd"] or 0)))

    confirmed = [r for r in out_rows if r["tier"].startswith("PEER_CLUSTER")]

    out = {
        "version": VERSION,
        "generated_at": started.isoformat(),
        "source_feed": source,
        "lookback_days": lookback,
        "n_industries": len(out_rows),
        "n_clusters": len(confirmed),
        "industries": out_rows,
        "degraded": degraded,
        "coverage": {
            "insider_rows_in": len(rows),
            "tickers_unmapped": unmapped,
            "universe_industries": len(listed),
            "min_companies": MIN_COMPANIES,
            "strong_companies": STRONG_COMPANIES,
            "min_listed_for_rate": MIN_LISTED,
        },
        "method": ("Breadth is DISTINCT COMPANIES, not transactions or dollars — "
                   "counting transactions rewards one insider filing five times "
                   "and counting dollars rewards a single large buyer. "
                   "Participation rate (buyers / listed) and a self-building "
                   "base-rate z guard the industries that always look active "
                   "(biotech, regional banks). Dollar HHI is published so a "
                   "cluster carried by one name is visibly labelled "
                   "CONCENTRATED rather than sold as breadth."),
        "attribution": "SEC EDGAR Form 4 via justhodl-insider-cluster-scanner; "
                       "industry map from justhodl universe",
    }

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":"), default=str),
                  ContentType="application/json")

    for ind, rec in hist["obs"].items():
        if len(rec) > 180:
            for old in sorted(rec)[:-180]:
                rec.pop(old, None)
    hist["updated_at"] = started.isoformat()
    S3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps(hist, separators=(",", ":")),
                  ContentType="application/json")

    print("[iic] rows_in=%d industries=%d clusters=%d unmapped=%d source=%s"
          % (len(rows), len(out_rows), len(confirmed), unmapped, source))
    return {"statusCode": 200, "body": json.dumps(
        {"industries": len(out_rows), "clusters": len(confirmed),
         "source": source})}
